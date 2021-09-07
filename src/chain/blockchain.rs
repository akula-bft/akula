use super::validity::ValidationError;
use crate::{
    chain::{consensus::Consensus, validity::pre_validate_block},
    execution::processor::ExecutionProcessor,
    models::*,
    state::*,
};
use anyhow::Context;
use async_recursion::async_recursion;
use ethereum_types::*;
use std::{collections::HashMap, convert::TryFrom, marker::PhantomData};

#[derive(Debug)]
pub struct Blockchain<'storage: 'state, 'state, S>
where
    S: State<'storage>,
{
    state: &'state mut S,
    config: ChainConfig,
    bad_blocks: HashMap<H256, ValidationError>,
    receipts: Vec<Receipt>,
    _marker: PhantomData<&'storage ()>,
}

impl<'storage: 'state, 'state, S> Blockchain<'storage, 'state, S>
where
    S: State<'storage>,
{
    pub async fn new(
        state: &'state mut S,
        config: ChainConfig,
        genesis_block: Block,
    ) -> anyhow::Result<Blockchain<'storage, 'state, S>> {
        let hash = genesis_block.header.hash();
        let number = genesis_block.header.number;
        state.insert_block(genesis_block, hash).await?;
        state.canonize_block(number, hash).await?;

        Ok(Self {
            state,
            config,
            bad_blocks: Default::default(),
            receipts: Default::default(),
            _marker: PhantomData,
        })
    }

    pub async fn insert_block<C>(
        &mut self,
        consensus: &C,
        block: Block,
        check_state_root: bool,
    ) -> anyhow::Result<()>
    where
        C: Consensus,
    {
        pre_validate_block(consensus, &block, self.state, &self.config).await?;

        let hash = block.header.hash();
        if let Some(error) = self.bad_blocks.get(&hash) {
            return Err(error.clone().into());
        }

        let b = BlockWithSenders::from(block.clone());

        let ancestor = self.canonical_ancestor(&b.header, hash).await?;

        let current_canonical_block = self.state.current_canonical_block().await?;

        self.unwind_last_changes(ancestor, current_canonical_block)
            .await?;

        let block_number = b.header.number;

        let mut chain = self
            .intermediate_chain(block_number - 1, b.header.parent_hash, ancestor)
            .await?;
        chain.push(WithHash { inner: b, hash });

        let mut num_of_executed_chain_blocks = 0;
        for x in &chain {
            if let Err(e) = self
                .execute_block(&x.inner, check_state_root)
                .await
                .with_context(|| format!("Failed to execute block #{}", block_number))
            {
                if let Some(e) = e.downcast_ref::<ValidationError>() {
                    self.bad_blocks.insert(hash, e.clone());
                    self.unwind_last_changes(ancestor, ancestor + num_of_executed_chain_blocks)
                        .await?;
                    self.re_execute_canonical_chain(ancestor, current_canonical_block)
                        .await?;
                }

                return Err(e);
            }

            num_of_executed_chain_blocks += 1;
        }

        self.state.insert_block(block, hash).await?;

        let current_total_difficulty = self
            .state
            .total_difficulty(
                current_canonical_block,
                self.state
                    .canonical_hash(current_canonical_block)
                    .await?
                    .unwrap(),
            )
            .await?
            .unwrap();

        if self
            .state
            .total_difficulty(block_number, hash)
            .await?
            .unwrap()
            > current_total_difficulty
        {
            // canonize the new chain
            for i in (ancestor + 1..=current_canonical_block).rev() {
                self.state.decanonize_block(i).await?;
            }

            for x in chain {
                self.state.canonize_block(x.header.number, x.hash).await?;
            }
        } else {
            self.unwind_last_changes(ancestor, ancestor + num_of_executed_chain_blocks)
                .await?;
            self.re_execute_canonical_chain(ancestor, current_canonical_block)
                .await?;
        }

        Ok(())
    }

    async fn execute_block(
        &mut self,
        block: &BlockWithSenders,
        check_state_root: bool,
    ) -> anyhow::Result<()> {
        let body = BlockBodyWithSenders {
            transactions: block.transactions.clone(),
            ommers: block.ommers.clone(),
        };

        let processor = ExecutionProcessor::new(self.state, &block.header, &body, &self.config);

        let _ = processor.execute_and_write_block().await?;

        if check_state_root {
            let state_root = self.state.state_root_hash().await?;
            if state_root != block.header.state_root {
                self.state.unwind_state_changes(block.header.number).await?;
                return Err(ValidationError::WrongStateRoot {
                    expected: block.header.state_root,
                    got: state_root,
                }
                .into());
            }
        }

        Ok(())
    }

    async fn re_execute_canonical_chain(&mut self, ancestor: u64, tip: u64) -> anyhow::Result<()> {
        assert!(ancestor <= tip);
        for block_number in ancestor + 1..=tip {
            let hash = self.state.canonical_hash(block_number).await?.unwrap();
            let body = self
                .state
                .read_body_with_senders(block_number, hash)
                .await?
                .unwrap();
            let header = self.state.read_header(block_number, hash).await?.unwrap();

            let block = BlockWithSenders {
                header: header.into(),
                transactions: body.transactions,
                ommers: body.ommers,
            };

            let _ = self.execute_block(&block, false).await.unwrap();
        }

        Ok(())
    }

    async fn unwind_last_changes(&mut self, ancestor: u64, tip: u64) -> anyhow::Result<()> {
        assert!(ancestor <= tip);
        for block_number in (ancestor + 1..=tip).rev() {
            self.state.unwind_state_changes(block_number).await?;
        }

        Ok(())
    }

    async fn intermediate_chain(
        &self,
        block_number: u64,
        mut hash: H256,
        canonical_ancestor: u64,
    ) -> anyhow::Result<Vec<WithHash<BlockWithSenders>>> {
        let mut chain =
            Vec::with_capacity(usize::try_from(block_number - canonical_ancestor).unwrap());
        for block_number in (canonical_ancestor + 1..=block_number).rev() {
            let body = self
                .state
                .read_body_with_senders(block_number, hash)
                .await?
                .unwrap();
            let header = self
                .state
                .read_header(block_number, hash)
                .await?
                .unwrap()
                .into();

            let block = WithHash {
                inner: BlockWithSenders {
                    header,
                    transactions: body.transactions,
                    ommers: body.ommers,
                },
                hash,
            };

            hash = block.header.parent_hash;

            chain.push(block);
        }

        chain.reverse();

        Ok(chain)
    }

    #[async_recursion]
    async fn canonical_ancestor(&self, header: &PartialHeader, hash: H256) -> anyhow::Result<u64> {
        if let Some(canonical_hash) = self.state.canonical_hash(header.number).await? {
            if canonical_hash == hash {
                return Ok(header.number);
            }
        }
        let parent = self
            .state
            .read_header(header.number - 1, header.parent_hash)
            .await?
            .ok_or(ValidationError::UnknownParent)?;
        self.canonical_ancestor(&parent.into(), header.parent_hash)
            .await
    }
}
