use crate::{
    bitmapdb::{self, CHUNK_LIMIT},
    etl::collector::*,
    kv::{
        mdbx::*,
        tables::{self, BitmapKey, CallTraceSetEntry},
        traits::*,
    },
    models::*,
    stagedsync::{stage::*, stages::*},
    StageId,
};
use anyhow::format_err;
use async_trait::async_trait;
use itertools::Itertools;
use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
    time::{Duration, Instant},
};
use tempfile::TempDir;
use tokio::pin;
use tracing::info;

/// Generate call trace index
#[derive(Debug)]
pub struct CallTraceIndex {
    pub temp_dir: Arc<TempDir>,
    pub flush_interval: u64,
}

#[async_trait]
impl<'db, E> Stage<'db, E> for CallTraceIndex
where
    E: EnvironmentKind,
{
    fn id(&self) -> StageId {
        CALL_TRACES
    }

    async fn execute<'tx>(
        &mut self,
        tx: &'tx mut MdbxTransaction<'db, RW, E>,
        input: StageInput,
    ) -> anyhow::Result<ExecOutput>
    where
        'db: 'tx,
    {
        let starting_block = input.stage_progress.unwrap_or(BlockNumber(0));
        let max_block = input
            .previous_stage
            .ok_or_else(|| format_err!("Call trace index generation cannot be the first stage"))?
            .1;

        let call_trace_set_cursor = tx.cursor(tables::CallTraceSet)?;
        let walker = call_trace_set_cursor.walk(Some(starting_block + 1));
        pin!(walker);

        let mut froms = HashMap::<Address, croaring::Treemap>::new();
        let mut tos = HashMap::<Address, croaring::Treemap>::new();

        let mut froms_collector =
            Collector::<Address, croaring::Treemap>::new(&*self.temp_dir, OPTIMAL_BUFFER_CAPACITY);
        let mut tos_collector =
            Collector::<Address, croaring::Treemap>::new(&*self.temp_dir, OPTIMAL_BUFFER_CAPACITY);

        fn flush(
            collector: &mut Collector<Address, croaring::Treemap>,
            src: &mut HashMap<Address, croaring::Treemap>,
        ) {
            for (address, index) in src.drain() {
                collector.push(address, index);
            }
        }

        let mut highest_block = starting_block;
        let mut last_flush = starting_block;

        let mut printed = false;
        let mut last_log = Instant::now();
        while let Some((block_number, CallTraceSetEntry { address, from, to })) =
            walker.next().transpose()?
        {
            if block_number > max_block {
                break;
            }

            if from {
                froms.entry(address).or_default().add(block_number.0);
            }

            if to {
                tos.entry(address).or_default().add(block_number.0);
            }

            if highest_block != block_number {
                highest_block = block_number;

                if highest_block.0 - last_flush.0 >= self.flush_interval {
                    flush(&mut froms_collector, &mut froms);
                    flush(&mut tos_collector, &mut tos);

                    last_flush = highest_block;
                }
            }

            let now = Instant::now();
            if last_log - now > Duration::from_secs(30) {
                info!("Current block: {}", block_number);
                printed = true;
                last_log = now;
            }
        }

        flush(&mut froms_collector, &mut froms);
        flush(&mut tos_collector, &mut tos);

        if printed {
            info!("Flushing froms index");
        }
        load_call_traces(&mut tx.cursor(tables::CallFromIndex)?, froms_collector)?;

        if printed {
            info!("Flushing tos index");
        }
        load_call_traces(&mut tx.cursor(tables::CallToIndex)?, tos_collector)?;

        Ok(ExecOutput::Progress {
            stage_progress: max_block,
            done: true,
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
        let call_trace_set_cursor = tx.cursor(tables::CallTraceSet)?;

        let mut to_addresses = BTreeSet::<Address>::new();
        let mut from_addresses = BTreeSet::<Address>::new();

        let walker = call_trace_set_cursor.walk(Some(input.unwind_to + 1));
        pin!(walker);
        while let Some((_, entry)) = walker.next().transpose()? {
            if entry.to {
                to_addresses.insert(entry.address);
            }

            if entry.from {
                from_addresses.insert(entry.address);
            }
        }

        unwind_call_traces(
            &mut tx.cursor(tables::CallFromIndex)?,
            from_addresses,
            input.unwind_to,
        )?;
        unwind_call_traces(
            &mut tx.cursor(tables::CallToIndex)?,
            to_addresses,
            input.unwind_to,
        )?;

        Ok(UnwindOutput {
            stage_progress: input.unwind_to,
        })
    }

    async fn prune<'tx>(
        &mut self,
        tx: &'tx mut MdbxTransaction<'db, RW, E>,
        input: PruningInput,
    ) -> anyhow::Result<()>
    where
        'db: 'tx,
    {
        let call_trace_set_cursor = tx.cursor(tables::CallTraceSet)?;

        let mut to_addresses = BTreeSet::<Address>::new();
        let mut from_addresses = BTreeSet::<Address>::new();

        let walker = call_trace_set_cursor.walk(None);
        pin!(walker);
        while let Some((b, entry)) = walker.next().transpose()? {
            if b >= input.prune_to {
                break;
            }

            if entry.to {
                to_addresses.insert(entry.address);
            }

            if entry.from {
                from_addresses.insert(entry.address);
            }
        }

        prune_call_traces(
            &mut tx.cursor(tables::CallFromIndex)?,
            from_addresses,
            input.prune_to,
        )?;
        prune_call_traces(
            &mut tx.cursor(tables::CallToIndex)?,
            to_addresses,
            input.prune_to,
        )?;

        Ok(())
    }
}

fn load_call_traces<T>(
    cursor: &mut MdbxCursor<'_, RW, T>,
    mut collector: Collector<'_, Address, croaring::Treemap>,
) -> anyhow::Result<()>
where
    T: Table<Key = BitmapKey<Address>, Value = croaring::Treemap>,
{
    for res in collector
        .iter()
        .map(|res| {
            let (address, bitmap) = res?;

            let address = Address::decode(&address)?;
            let bitmap = croaring::Treemap::decode(&bitmap)?;

            Ok::<_, anyhow::Error>((address, bitmap))
        })
        .coalesce(|prev, current| match (prev, current) {
            (Ok((prev_address, prev_bitmap)), Ok((current_address, current_bitmap))) => {
                if prev_address == current_address {
                    Ok(Ok((prev_address, prev_bitmap | current_bitmap)))
                } else {
                    Err((
                        Ok((prev_address, prev_bitmap)),
                        Ok((current_address, current_bitmap)),
                    ))
                }
            }
            err => Err(err),
        })
    {
        let (address, mut total_bitmap) = res?;

        if !total_bitmap.is_empty() {
            if let Some((_, last_bitmap)) = cursor.seek_exact(BitmapKey {
                inner: address,
                block_number: BlockNumber(u64::MAX),
            })? {
                total_bitmap |= last_bitmap;
            }

            for (block_number, bitmap) in
                bitmapdb::Chunks::new(total_bitmap, CHUNK_LIMIT).with_keys()
            {
                cursor.put(
                    BitmapKey {
                        inner: address,
                        block_number,
                    },
                    bitmap,
                )?;
            }
        }
    }

    Ok(())
}

fn unwind_call_traces<T>(
    cursor: &mut MdbxCursor<'_, RW, T>,
    addresses: BTreeSet<Address>,
    unwind_to: BlockNumber,
) -> anyhow::Result<()>
where
    T: Table<Key = BitmapKey<Address>, Value = croaring::Treemap>,
{
    for address in addresses {
        let mut bm = cursor
            .seek_exact(BitmapKey {
                inner: address,
                block_number: BlockNumber(u64::MAX),
            })?
            .map(|(_, bm)| bm);

        while let Some(b) = bm {
            cursor.delete_current()?;

            let new_bm = b
                .iter()
                .take_while(|&v| v <= *unwind_to)
                .collect::<croaring::Treemap>();

            if new_bm.cardinality() > 0 {
                cursor.upsert(
                    BitmapKey {
                        inner: address,
                        block_number: BlockNumber(u64::MAX),
                    },
                    new_bm,
                )?;
            }

            bm = cursor.prev()?.and_then(
                |(BitmapKey { inner, .. }, b)| if inner == address { Some(b) } else { None },
            );
        }
    }

    Ok(())
}

fn prune_call_traces<T>(
    cursor: &mut MdbxCursor<'_, RW, T>,
    addresses: BTreeSet<Address>,
    prune_to: BlockNumber,
) -> anyhow::Result<()>
where
    T: Table<Key = BitmapKey<Address>, Value = croaring::Treemap, SeekKey = BitmapKey<Address>>,
{
    for address in addresses {
        let mut bm = cursor.seek(BitmapKey {
            inner: address,
            block_number: BlockNumber(0),
        })?;

        while let Some((
            BitmapKey {
                inner,
                block_number,
            },
            b,
        )) = bm
        {
            if inner != address {
                break;
            }

            cursor.delete_current()?;

            if block_number >= prune_to {
                let new_bm = b
                    .iter()
                    .skip_while(|&v| v < *prune_to)
                    .collect::<croaring::Treemap>();

                if new_bm.cardinality() > 0 {
                    cursor.upsert(
                        BitmapKey {
                            inner: address,
                            block_number,
                        },
                        new_bm,
                    )?;
                }

                break;
            }

            bm = cursor.next()?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    #[tokio::test]
    async fn call_traces() {
        let db = crate::kv::new_mem_database().unwrap();

        let mut tx = db.begin_mutable().unwrap();

        for i in 0..30 {
            let mut address = [0; 20];
            address[19] = u8::try_from(i % 5).unwrap();

            tx.set(
                tables::CallTraceSet,
                BlockNumber(i),
                CallTraceSetEntry {
                    address: address.into(),
                    from: i % 2 == 0,
                    to: i % 2 == 1,
                },
            )
            .unwrap();
        }

        let mut address = Address::zero();
        address.0[19] = 1;

        fn froms<K: TransactionKind, E: EnvironmentKind>(
            tx: &MdbxTransaction<'_, K, E>,
            address: Address,
        ) -> croaring::Treemap {
            bitmapdb::get(
                tx,
                tables::CallFromIndex,
                address,
                BlockNumber(0)..=BlockNumber(30),
            )
            .unwrap()
        }

        fn tos<K: TransactionKind, E: EnvironmentKind>(
            tx: &MdbxTransaction<'_, K, E>,
            address: Address,
        ) -> croaring::Treemap {
            bitmapdb::get(
                tx,
                tables::CallToIndex,
                address,
                BlockNumber(0)..=BlockNumber(30),
            )
            .unwrap()
        }

        let stage = || CallTraceIndex {
            temp_dir: Arc::new(TempDir::new().unwrap()),
            flush_interval: 0,
        };

        assert_eq!(
            (stage)()
                .execute(
                    &mut tx,
                    StageInput {
                        restarted: false,
                        first_started_at: (Instant::now(), Some(BlockNumber(0))),
                        previous_stage: Some((EXECUTION, BlockNumber(20))),
                        stage_progress: None,
                    },
                )
                .await
                .unwrap(),
            ExecOutput::Progress {
                stage_progress: BlockNumber(20),
                done: true,
            }
        );

        assert_eq!(
            vec![6, 16],
            (froms)(&tx, address).iter().collect::<Vec<_>>()
        );
        assert_eq!(vec![1, 11], (tos)(&tx, address).iter().collect::<Vec<_>>());

        (stage)()
            .unwind(
                &mut tx,
                UnwindInput {
                    stage_progress: BlockNumber(20),
                    unwind_to: BlockNumber(10),
                },
            )
            .await
            .unwrap();

        assert_eq!(vec![6], (froms)(&tx, address).iter().collect::<Vec<_>>());
        assert_eq!(vec![1], (tos)(&tx, address).iter().collect::<Vec<_>>());

        assert_eq!(
            (stage)()
                .execute(
                    &mut tx,
                    StageInput {
                        restarted: false,
                        first_started_at: (Instant::now(), Some(BlockNumber(10))),
                        previous_stage: Some((EXECUTION, BlockNumber(30))),
                        stage_progress: Some(BlockNumber(10)),
                    },
                )
                .await
                .unwrap(),
            ExecOutput::Progress {
                stage_progress: BlockNumber(30),
                done: true,
            }
        );

        assert_eq!(
            vec![6, 16, 26],
            (froms)(&tx, address).iter().collect::<Vec<_>>()
        );
        assert_eq!(
            vec![1, 11, 21],
            (tos)(&tx, address).iter().collect::<Vec<_>>()
        );
    }
}
