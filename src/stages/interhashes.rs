use crate::{
    consensus::DuoError,
    kv::{mdbx::*, tables},
    models::*,
    stagedsync::{
        stage::{ExecOutput, Stage, StageInput, UnwindInput, UnwindOutput},
        stages::*,
    },
    stages::stage_util::should_do_clean_promotion,
    trie::{increment_intermediate_hashes, regenerate_intermediate_hashes},
    StageId,
};
use anyhow::{format_err, Context};
use async_trait::async_trait;
use std::{cmp, sync::Arc};
use tempfile::TempDir;
use tracing::*;

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
    ) -> anyhow::Result<ExecOutput>
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
            let block_state_root = tx
                .get(
                    tables::Header,
                    (
                        max_block,
                        tx.get(tables::CanonicalHeader, max_block)?.ok_or_else(|| {
                            format_err!("No canonical hash for block {}", max_block)
                        })?,
                    ),
                )?
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
                    .with_context(|| "Failed to generate interhashes")?
            } else {
                debug!("Incrementing intermediate hashes");
                let res = increment_intermediate_hashes(
                    tx,
                    self.temp_dir.as_ref(),
                    past_progress,
                    Some(block_state_root),
                );
                if let Err(DuoError::Validation(_)) = &res {
                    warn!(
                        "Failed to increment intermediate hashes: {res:?}. Attempting regenerate."
                    );

                    regenerate_intermediate_hashes(
                        tx,
                        self.temp_dir.as_ref(),
                        Some(block_state_root),
                    )
                    .with_context(|| "Failed to generate interhashes")?
                } else {
                    res.with_context(|| "Failed to increment interhashes")?
                }
            };

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
        let _ = input;
        // TODO: proper unwind
        tx.clear_table(tables::TrieAccount)?;
        tx.clear_table(tables::TrieStorage)?;

        Ok(UnwindOutput {
            stage_progress: BlockNumber(0),
        })
    }
}
