use crate::{
    accessors,
    consensus::DuoError,
    kv::mdbx::*,
    models::*,
    stagedsync::stage::{ExecOutput, Stage, StageError, StageInput, UnwindInput, UnwindOutput},
    stages::stage_util::should_do_clean_promotion,
    trie::*,
    StageId,
};
use anyhow::format_err;
use async_trait::async_trait;
use std::{cmp, sync::Arc};
use tempfile::TempDir;
use tracing::*;

pub const INTERMEDIATE_HASHES: StageId = StageId("IntermediateHashes");

/// Generation of intermediate hashes for efficient computation of the state trie root
#[derive(Debug)]
pub struct Interhashes {
    temp_dir: Arc<TempDir>,
    clean_promotion_threshold: u64,
}

impl Interhashes {
    pub fn new(temp_dir: Arc<TempDir>, clean_promotion_threshold: Option<u64>) -> Self {
        Self {
            temp_dir,
            clean_promotion_threshold: clean_promotion_threshold.unwrap_or(1_000_000_000_000),
        }
    }
}

#[async_trait]
impl<'db, E> Stage<'db, E> for Interhashes
where
    E: EnvironmentKind,
{
    fn id(&self) -> StageId {
        INTERMEDIATE_HASHES
    }

    async fn execute<'tx>(
        &mut self,
        tx: &'tx mut MdbxTransaction<'db, RW, E>,
        input: StageInput,
    ) -> Result<ExecOutput, StageError>
    where
        'db: 'tx,
    {
        let genesis = BlockNumber(0);
        let max_block = input
            .previous_stage
            .map(|tuple| tuple.1)
            .ok_or_else(|| format_err!("Cannot be first stage"))?;
        let past_progress = input.stage_progress.unwrap_or(genesis);

        if max_block > past_progress {
            let block_state_root = accessors::chain::header::read(tx, max_block)?
                .ok_or_else(|| format_err!("No header for block {}", max_block))?
                .state_root;

            let trie_root = if should_do_clean_promotion(
                tx,
                genesis,
                past_progress,
                max_block,
                self.clean_promotion_threshold,
            )? {
                debug!("Regenerating intermediate hashes");
                regenerate_intermediate_hashes(tx, self.temp_dir.as_ref(), Some(block_state_root))
            } else {
                debug!("Incrementing intermediate hashes");
                increment_intermediate_hashes(
                    tx,
                    self.temp_dir.as_ref(),
                    past_progress,
                    Some(block_state_root),
                )
            }
            .map_err(|e| match e {
                DuoError::Validation(error) => StageError::Validation {
                    block: max_block,
                    error,
                },
                DuoError::Internal(e) => {
                    StageError::Internal(e.context("state root computation failure"))
                }
            })?;

            info!("Block #{} state root OK: {:?}", max_block, trie_root)
        };

        Ok(ExecOutput::Progress {
            stage_progress: cmp::max(max_block, past_progress),
            done: true,
            reached_tip: true,
        })
    }

    async fn unwind<'tx>(
        &mut self,
        tx: &'tx mut MdbxTransaction<'db, RW, E>,
        input: UnwindInput,
    ) -> anyhow::Result<UnwindOutput>
    where
        'db: 'tx,
    {
        let block_state_root = accessors::chain::header::read(tx, input.unwind_to)?
            .ok_or_else(|| format_err!("No header for block {}", input.unwind_to))?
            .state_root;

        unwind_intermediate_hashes(
            tx,
            self.temp_dir.as_ref(),
            input.unwind_to,
            Some(block_state_root),
        )?;

        Ok(UnwindOutput {
            stage_progress: input.unwind_to,
        })
    }
}
