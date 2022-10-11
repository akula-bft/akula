use crate::{
    kv::{mdbx::*, tables, traits::TryGenIter},
    models::*,
    snapshot::{SnapshotObject, SnapshotVersion, Snapshotter, V1},
    stagedsync::stage::*,
    StageId,
};
use anyhow::format_err;
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::{mpsc::Sender, Mutex as AsyncMutex};
use tracing::*;

const MIN_DISTANCE: usize = 1_000;

pub const HEADER_SNAPSHOT: StageId = StageId("HeaderSnapshot");
pub const BODY_SNAPSHOT: StageId = StageId("BodySnapshot");
pub const SENDER_SNAPSHOT: StageId = StageId("SenderSnapshot");

impl SnapshotObject for BlockHeader {
    const ID: &'static str = "headers";
    type Table = crate::kv::tables::HeaderSnapshot;

    fn db_table() -> Self::Table {
        crate::kv::tables::HeaderSnapshot
    }
}

impl SnapshotObject for BlockBody {
    const ID: &'static str = "bodies";
    type Table = crate::kv::tables::BodySnapshot;

    fn db_table() -> Self::Table {
        crate::kv::tables::BodySnapshot
    }
}

impl SnapshotObject for Vec<Address> {
    const ID: &'static str = "senders";
    type Table = crate::kv::tables::SenderSnapshot;

    fn db_table() -> Self::Table {
        crate::kv::tables::SenderSnapshot
    }
}

#[derive(Debug)]
pub struct HeaderSnapshot {
    pub snapshotter: Arc<AsyncMutex<Snapshotter<V1, BlockHeader>>>,
    pub bt_sender: Sender<(H160, Option<Vec<u8>>)>,
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
            tx,
            &mut self.bt_sender,
            |last_snapshotted_block| {
                Ok(tx
                    .cursor(tables::Header)?
                    .walk(last_snapshotted_block.map(|v| v + 1)))
            },
            prev_stage_progress,
        )
        .await?;

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

#[derive(Debug)]
pub struct BodySnapshot {
    pub snapshotter: Arc<AsyncMutex<Snapshotter<V1, BlockBody>>>,
    pub bt_sender: Sender<(H160, Option<Vec<u8>>)>,
}

#[async_trait]
impl<'db, E> Stage<'db, E> for BodySnapshot
where
    E: EnvironmentKind,
{
    fn id(&self) -> StageId {
        BODY_SNAPSHOT
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
            tx,
            &mut self.bt_sender,
            |last_snapshotted_block| {
                let tx = &*tx;
                Ok(TryGenIter::from(move || {
                    let walk_from = last_snapshotted_block
                        .map(|v| v + 1)
                        .unwrap_or(BlockNumber(0));
                    for number in walk_from.. {
                        if let Some(v) =
                            crate::accessors::chain::block_body::read_without_senders(tx, number)?
                        {
                            yield (number, v);
                        } else {
                            break;
                        }
                    }

                    Ok::<_, anyhow::Error>(())
                }))
            },
            prev_stage_progress,
        )
        .await?;

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

#[derive(Debug)]
pub struct SenderSnapshot {
    pub snapshotter: Arc<AsyncMutex<Snapshotter<V1, Vec<Address>>>>,
    pub bt_sender: Sender<(H160, Option<Vec<u8>>)>,
}

#[async_trait]
impl<'db, E> Stage<'db, E> for SenderSnapshot
where
    E: EnvironmentKind,
{
    fn id(&self) -> StageId {
        SENDER_SNAPSHOT
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
            tx,
            &mut self.bt_sender,
            |last_snapshotted_block| {
                let tx = &*tx;
                Ok(TryGenIter::from(move || {
                    let walk_from = last_snapshotted_block
                        .map(|v| v + 1)
                        .unwrap_or(BlockNumber(0));
                    for number in walk_from.. {
                        if crate::accessors::chain::canonical_hash::read(tx, number)?.is_some() {
                            yield (
                                number,
                                crate::accessors::chain::tx_sender::read(tx, number)?,
                            );
                        } else {
                            break;
                        }
                    }

                    Ok::<_, anyhow::Error>(())
                }))
            },
            prev_stage_progress,
        )
        .await?;

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

async fn execute_snapshot<Version, T, IT, E>(
    snapshotter: &mut Snapshotter<Version, T>,
    tx: &MdbxTransaction<'_, RW, E>,
    bt_sender: &mut Sender<(H160, Option<Vec<u8>>)>,
    mut extractor: impl FnMut(Option<BlockNumber>) -> anyhow::Result<IT>,
    prev_stage_progress: BlockNumber,
) -> anyhow::Result<()>
where
    Version: SnapshotVersion,
    T: SnapshotObject,
    IT: Iterator<Item = anyhow::Result<(BlockNumber, T)>>,
    E: EnvironmentKind,
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

            let (next_snapshot_idx, snapshot_path) =
                snapshotter.snapshot((extractor)(last_snapshotted_block)?)?;

            let torrent =
                lava_torrent::torrent::v1::TorrentBuilder::new(&snapshot_path, 2_i64.pow(18))
                    .build()
                    .unwrap();

            let info_hash = H160::from_slice(&torrent.info_hash_bytes());
            let torrent_encoded = torrent.encode()?;
            tx.cursor(T::db_table())?
                .append(next_snapshot_idx as u64, info_hash)?;
            tx.set(tables::Torrents, info_hash, torrent_encoded.clone())?;

            let _ = bt_sender.send((info_hash, Some(torrent_encoded))).await;
        }
    }

    Ok(())
}
