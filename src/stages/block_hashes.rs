use crate::{
    etl::collector::*,
    kv::{mdbx::*, tables},
    models::*,
    stagedsync::stage::*,
    StageId,
};
use async_trait::async_trait;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::pin;
use tracing::*;

pub const BLOCK_HASHES: StageId = StageId("BlockHashes");

/// Generate BlockHashes => BlockNumber Mapping
#[derive(Debug)]
pub struct BlockHashes {
    pub temp_dir: Arc<TempDir>,
}

#[async_trait]
impl<'db, E> Stage<'db, E> for BlockHashes
where
    E: EnvironmentKind,
{
    fn id(&self) -> StageId {
        BLOCK_HASHES
    }

    async fn execute<'tx>(
        &mut self,
        tx: &'tx mut MdbxTransaction<'db, RW, E>,
        input: StageInput,
    ) -> Result<ExecOutput, StageError>
    where
        'db: 'tx,
    {
        let original_highest_block = input.stage_progress.unwrap_or(BlockNumber(0));
        let mut highest_block = original_highest_block;

        let bodies_cursor = tx.cursor(tables::CanonicalHeader)?;
        let mut blockhashes_cursor = tx.cursor(tables::HeaderNumber.erased())?;

        let mut collector = TableCollector::new(&self.temp_dir, OPTIMAL_BUFFER_CAPACITY);
        let walker = bodies_cursor.walk(Some(highest_block + 1));
        pin!(walker);

        while let Some((block_number, block_hash)) = walker.next().transpose()? {
            if block_number.0 % 500_000 == 0 {
                info!("Processing block hashes (at block {})", block_number);
            } else if block_number.0 % 10_000 == 0 {
                debug!("[block_hashes:execute] Processing block hahes (at {})", block_number);
            }

            // BlockBody Key is block_number + hash, so we just separate and collect
            collector.push(block_hash, block_number);

            highest_block = block_number;
        }
        collector.load(&mut blockhashes_cursor)?;
        Ok(ExecOutput::Progress {
            stage_progress: highest_block,
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
        let mut header_number_cur = tx.cursor(tables::HeaderNumber)?;

        let walker = tx.cursor(tables::CanonicalHeader)?.walk_back(None);
        pin!(walker);

        while let Some((block_num, block_hash)) = walker.next().transpose()? {
            if block_num > input.unwind_to {
                if header_number_cur.seek(block_hash)?.is_some() {
                    header_number_cur.delete_current()?;
                }
            } else {
                break;
            }
        }

        Ok(UnwindOutput {
            stage_progress: input.unwind_to,
        })
    }
}
