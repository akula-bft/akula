use crate::{
    accessors,
    kv::{tables, traits::MutableTransaction},
    models::*,
    stagedsync::stage::{ExecOutput, Stage, StageInput, UnwindInput, UnwindOutput},
    stages::stage_util::should_do_clean_promotion,
    trie::{increment_intermediate_hashes, regenerate_intermediate_hashes},
    StageId,
};
use anyhow::{format_err, Context};
use async_trait::async_trait;
use std::{cmp, sync::Arc};
use tempfile::TempDir;
use tracing::info;

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
impl<'db, RwTx> Stage<'db, RwTx> for Interhashes
where
    RwTx: MutableTransaction<'db>,
{
    fn id(&self) -> StageId {
        StageId("Interhashes")
    }

    fn description(&self) -> &'static str {
        "Generating intermediate hashes for efficient computation of the trie root"
    }

    async fn execute<'tx>(&self, tx: &'tx mut RwTx, input: StageInput) -> anyhow::Result<ExecOutput>
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
            let block_state_root = accessors::chain::header::read(
                tx,
                accessors::chain::canonical_hash::read(tx, max_block)
                    .await?
                    .ok_or_else(|| format_err!("No canonical hash for block {}", max_block))?,
                max_block,
            )
            .await?
            .ok_or_else(|| format_err!("No header for block {}", max_block))?
            .state_root;

            let trie_root = if should_do_clean_promotion(
                tx,
                genesis,
                past_progress,
                max_block,
                self.clean_promotion_threshold,
            )
            .await?
            {
                regenerate_intermediate_hashes(tx, self.temp_dir.as_ref(), Some(block_state_root))
                    .await
                    .with_context(|| "Failed to generate interhashes")?
            } else {
                increment_intermediate_hashes(
                    tx,
                    self.temp_dir.as_ref(),
                    past_progress,
                    Some(block_state_root),
                )
                .await
                .with_context(|| "Failed to update interhashes")?
            };

            info!("Block #{} state root OK: {:?}", max_block, trie_root)
        };

        Ok(ExecOutput::Progress {
            stage_progress: cmp::max(max_block, past_progress),
            done: true,
        })
    }

    async fn unwind<'tx>(
        &self,
        tx: &'tx mut RwTx,
        input: UnwindInput,
    ) -> anyhow::Result<UnwindOutput>
    where
        'db: 'tx,
    {
        let _ = input;
        // TODO: proper unwind
        tx.clear_table(tables::TrieAccount).await?;
        tx.clear_table(tables::TrieStorage).await?;

        Ok(UnwindOutput {
            stage_progress: BlockNumber(0),
        })
    }
}
