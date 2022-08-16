use crate::{
    kv::{mdbx::*, tables, traits::TableObject},
    models::*,
    snapshot::Snapshotter,
    stagedsync::stage::*,
    StageId,
};
use anyhow::format_err;
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::Mutex as AsyncMutex;
use tracing::*;

const MIN_DISTANCE: usize = 90_000;

pub const HEADER_SNAPSHOT: StageId = StageId("HeaderSnapshot");

#[derive(Debug)]
pub struct HeaderSnapshot {
    pub snapshotter: Arc<AsyncMutex<Snapshotter<BlockHeader>>>,
}

#[async_trait]
impl<'db, E> Stage<'db, E> for HeaderSnapshot
where
    E: EnvironmentKind,
{
    fn id(&self) -> StageId {
        HEADER_SNAPSHOT
    }

    async fn execute<'tx>(
        &mut self,
        tx: &'tx mut MdbxTransaction<'db, RW, E>,
        input: StageInput,
    ) -> Result<ExecOutput, StageError>
    where
        'db: 'tx,
    {
        let prev_stage_progress = input
            .previous_stage
            .ok_or_else(|| format_err!("Cannot be the first stage"))?
            .1;

        let mut snapshotter = self.snapshotter.lock().await;
        execute_snapshot(
            &mut snapshotter,
            |last_snapshotted_block| {
                Ok(tx
                    .cursor(tables::Header)?
                    .walk(last_snapshotted_block.map(|v| v + 1))
                    .map(|res| res.map(|((block_number, _), header)| (block_number, header))))
            },
            prev_stage_progress,
        )?;

        Ok(ExecOutput::Progress {
            stage_progress: prev_stage_progress,
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
        let _ = tx;
        Ok(UnwindOutput {
            stage_progress: input.unwind_to,
        })
    }
}

fn execute_snapshot<T, IT>(
    snapshotter: &mut Snapshotter<T>,
    mut extractor: impl FnMut(Option<BlockNumber>) -> anyhow::Result<IT>,
    prev_stage_progress: BlockNumber,
) -> anyhow::Result<()>
where
    T: TableObject,
    IT: Iterator<Item = anyhow::Result<(BlockNumber, T)>>,
{
    if let Some(max_progress) = prev_stage_progress.checked_sub(MIN_DISTANCE as u64) {
        loop {
            let last_snapshotted_block = snapshotter.max_block();
            let next_last_snapshotted_block = snapshotter.next_max_block();
            if max_progress < next_last_snapshotted_block.0 {
                break;
            }
            debug!(
                "Snapshotting from block {} to {}",
                if let Some(b) = last_snapshotted_block {
                    format!("{b}")
                } else {
                    "genesis".to_string()
                },
                next_last_snapshotted_block,
            );
            snapshotter.snapshot((extractor)(last_snapshotted_block)?)?;
        }
    }

    Ok(())
}
