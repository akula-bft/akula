use crate::{
    consensus::*,
    execution::{analysis_cache::AnalysisCache, processor::ExecutionProcessor, tracer::NoopTracer},
    models::*,
    state::*,
};
use anyhow::format_err;
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
    pub fn new(
        state: &'state mut InMemoryState,
        config: ChainSpec,
        genesis_block: Block,
    ) -> anyhow::Result<Blockchain<'state>> {
        Self::new_with_consensus(
            state,
            engine_factory(None, config.clone(), None)?,
            config,
            genesis_block,
        )
    }

    pub fn new_with_consensus(
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

    pub fn insert_block(&mut self, block: Block, check_state_root: bool) -> Result<(), DuoError> {
        let parent = self
            .state
            .read_parent_header(&block.header)?
            .ok_or_else(|| format_err!("no parent header"))?;
        self.engine
            .validate_block_header(&block.header, &parent, true)?;
        self.engine.pre_validate_block(&block, &self.state)?;

        let hash = block.header.hash();
        if let Some(error) = self.bad_blocks.get(&hash) {
            return Err(error.clone().into());
        }

        let b = BlockWithSenders::from(block.clone());

        let ancestor = self.canonical_ancestor(&b.header, hash)?;

        let current_canonical_block = self.state.current_canonical_block();

        self.unwind_last_changes(ancestor, current_canonical_block)?;

        let block_number = b.header.number;

        let mut chain = self.intermediate_chain(
            BlockNumber(block_number.0 - 1),
            b.header.parent_hash,
            ancestor,
        )?;
        chain.push(WithHash { inner: b, hash });

        let mut num_of_executed_chain_blocks = 0;
        for x in &chain {
            self.execute_block(&x.inner, check_state_root)
                .or_else(|e| {
                    Err(match e {
                        DuoError::Validation(e) => {
                            self.bad_blocks.insert(hash, e.clone());
                            self.unwind_last_changes(
                                ancestor,
                                ancestor.0 + num_of_executed_chain_blocks,
                            )?;
                            self.re_execute_canonical_chain(ancestor, current_canonical_block)?;

                            DuoError::Validation(e)
                        }
                        DuoError::Internal(e) => DuoError::Internal(
                            e.context(format!("Failed to execute block #{}", block_number)),
                        ),
                    })
                })?;

            num_of_executed_chain_blocks += 1;
        }

        self.state.insert_block(block, hash);

        let current_total_difficulty = self
            .state
            .total_difficulty(
                current_canonical_block,
                self.state.canonical_hash(current_canonical_block).unwrap(),
            )
            .unwrap();

        if self.state.total_difficulty(block_number, hash).unwrap() > current_total_difficulty {
            // canonize the new chain
            for i in (ancestor + 1..=current_canonical_block).rev() {
                self.state.decanonize_block(i);
            }

            for x in chain {
                self.state.canonize_block(x.header.number, x.hash);
            }
        } else {
            self.unwind_last_changes(ancestor, ancestor + num_of_executed_chain_blocks)?;
            self.re_execute_canonical_chain(ancestor, current_canonical_block)?;
        }

        Ok(())
    }

    fn execute_block(
        &mut self,
        block: &BlockWithSenders,
        check_state_root: bool,
    ) -> Result<(), DuoError> {
        let body = BlockBodyWithSenders {
            transactions: block.transactions.clone(),
            ommers: block.ommers.clone(),
        };

        let block_spec = self.config.collect_block_spec(block.header.number);

        let mut analysis_cache = AnalysisCache::default();
        let mut tracer = NoopTracer;
        let processor = ExecutionProcessor::new(
            self.state,
            &mut tracer,
            &mut analysis_cache,
            &mut *self.engine,
            &block.header,
            &body,
            &block_spec,
            &self.config,
        );

        let _ = processor.execute_and_write_block()?;

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

    fn re_execute_canonical_chain(
        &mut self,
        ancestor: impl Into<BlockNumber>,
        tip: impl Into<BlockNumber>,
    ) -> Result<(), DuoError> {
        let ancestor = ancestor.into();
        let tip = tip.into();
        assert!(ancestor <= tip);
        for block_number in ancestor + 1..=tip {
            let hash = self.state.canonical_hash(block_number).unwrap();
            let body = self
                .state
                .read_body_with_senders(block_number, hash)?
                .unwrap();
            let header = self.state.read_header(block_number, hash)?.unwrap();

            let block = BlockWithSenders {
                header,
                transactions: body.transactions,
                ommers: body.ommers,
            };

            self.execute_block(&block, false).unwrap();
        }

        Ok(())
    }

    fn unwind_last_changes(
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

    fn intermediate_chain(
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
            let header = self.state.read_header(block_number, hash)?.unwrap();

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

    fn canonical_ancestor(
        &self,
        header: &BlockHeader,
        hash: H256,
    ) -> Result<BlockNumber, DuoError> {
        if let Some(canonical_hash) = self.state.canonical_hash(header.number) {
            if canonical_hash == hash {
                return Ok(header.number);
            }
        }
        let parent = self
            .state
            .read_header(BlockNumber(header.number.0 - 1), header.parent_hash)?
            .ok_or(ValidationError::UnknownParent {
                number: header.number,
                parent_hash: header.parent_hash,
            })?;
        self.canonical_ancestor(&parent, header.parent_hash)
    }
}
