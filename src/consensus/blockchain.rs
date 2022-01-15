use crate::{
    consensus::*,
    execution::{analysis_cache::AnalysisCache, processor::ExecutionProcessor},
    models::*,
    state::*,
};
use anyhow::Context;
use async_recursion::async_recursion;
use std::{collections::HashMap, convert::TryFrom};

#[derive(Debug)]
pub struct Blockchain<'state> {
    state: &'state mut InMemoryState,
    config: ChainSpec,
    engine: Box<dyn Consensus>,
    bad_blocks: HashMap<H256, ValidationError>,
    receipts: Vec<Receipt>,
}

impl<'state> Blockchain<'state> {
    pub async fn new(
        state: &'state mut InMemoryState,
        config: ChainSpec,
        genesis_block: Block,
    ) -> anyhow::Result<Blockchain<'state>> {
        Self::new_with_consensus(
            state,
            engine_factory(config.clone())?,
            config,
            genesis_block,
        )
        .await
    }

    pub async fn new_with_consensus(
        state: &'state mut InMemoryState,
        engine: Box<dyn Consensus>,
        config: ChainSpec,
        genesis_block: Block,
    ) -> anyhow::Result<Blockchain<'state>> {
        let hash = genesis_block.header.hash();
        let number = genesis_block.header.number;
        state.insert_block(genesis_block, hash);
        state.canonize_block(number, hash);

        Ok(Self {
            state,
            engine,
            config,
            bad_blocks: Default::default(),
            receipts: Default::default(),
        })
    }

    pub async fn insert_block(
        &mut self,
        block: Block,
        check_state_root: bool,
    ) -> anyhow::Result<()> {
        self.engine
            .validate_block_header(&block.header, &mut self.state, true)
            .await?;
        self.engine
            .pre_validate_block(&block, &mut self.state)
            .await?;

        let hash = block.header.hash();
        if let Some(error) = self.bad_blocks.get(&hash) {
            return Err(error.clone().into());
        }

        let b = BlockWithSenders::from(block.clone());

        let ancestor = self.canonical_ancestor(&b.header, hash).await?;

        let current_canonical_block = self.state.current_canonical_block();

        self.unwind_last_changes(ancestor, current_canonical_block)
            .await?;

        let block_number = b.header.number;

        let mut chain = self
            .intermediate_chain(
                BlockNumber(block_number.0 - 1),
                b.header.parent_hash,
                ancestor,
            )
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
                    self.unwind_last_changes(ancestor, ancestor.0 + num_of_executed_chain_blocks)
                        .await?;
                    self.re_execute_canonical_chain(ancestor, current_canonical_block)
                        .await?;
                }

                return Err(e);
            }

            num_of_executed_chain_blocks += 1;
        }

        self.state.insert_block(block, hash);

        let current_total_difficulty = self
            .state
            .total_difficulty(
                current_canonical_block,
                self.state.canonical_hash(current_canonical_block).unwrap(),
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
                self.state.decanonize_block(i);
            }

            for x in chain {
                self.state.canonize_block(x.header.number, x.hash);
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

        let block_spec = self.config.collect_block_spec(block.header.number);

        let mut analysis_cache = AnalysisCache::default();
        let processor = ExecutionProcessor::new(
            self.state,
            None,
            &mut analysis_cache,
            &mut *self.engine,
            &block.header,
            &body,
            &block_spec,
        );

        let _ = processor.execute_and_write_block().await?;

        if check_state_root {
            let state_root = self.state.state_root_hash();
            if state_root != block.header.state_root {
                self.state.unwind_state_changes(block.header.number);
                return Err(ValidationError::WrongStateRoot {
                    expected: block.header.state_root,
                    got: state_root,
                }
                .into());
            }
        }

        Ok(())
    }

    async fn re_execute_canonical_chain(
        &mut self,
        ancestor: impl Into<BlockNumber>,
        tip: impl Into<BlockNumber>,
    ) -> anyhow::Result<()> {
        let ancestor = ancestor.into();
        let tip = tip.into();
        assert!(ancestor <= tip);
        for block_number in ancestor + 1..=tip {
            let hash = self.state.canonical_hash(block_number).unwrap();
            let body = self
                .state
                .read_body_with_senders(block_number, hash)?
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

    async fn unwind_last_changes(
        &mut self,
        ancestor: impl Into<BlockNumber>,
        tip: impl Into<BlockNumber>,
    ) -> anyhow::Result<()> {
        let ancestor = ancestor.into();
        let tip = tip.into();
        assert!(ancestor <= tip);
        for block_number in (ancestor + 1..=tip).rev() {
            self.state.unwind_state_changes(block_number);
        }

        Ok(())
    }

    async fn intermediate_chain(
        &self,
        block_number: impl Into<BlockNumber>,
        mut hash: H256,
        canonical_ancestor: impl Into<BlockNumber>,
    ) -> anyhow::Result<Vec<WithHash<BlockWithSenders>>> {
        let block_number = block_number.into();
        let canonical_ancestor = canonical_ancestor.into();
        let mut chain =
            Vec::with_capacity(usize::try_from(block_number.0 - canonical_ancestor.0).unwrap());
        for block_number in (canonical_ancestor + 1..=block_number).rev() {
            let body = self
                .state
                .read_body_with_senders(block_number, hash)?
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
    async fn canonical_ancestor(
        &self,
        header: &PartialHeader,
        hash: H256,
    ) -> anyhow::Result<BlockNumber> {
        if let Some(canonical_hash) = self.state.canonical_hash(header.number) {
            if canonical_hash == hash {
                return Ok(header.number);
            }
        }
        let parent = self
            .state
            .read_header(BlockNumber(header.number.0 - 1), header.parent_hash)
            .await?
            .ok_or(ValidationError::UnknownParent)?;
        self.canonical_ancestor(&parent.into(), header.parent_hash)
            .await
    }
}
