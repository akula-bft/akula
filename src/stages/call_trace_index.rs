use crate::{
    etl::collector::*,
    kv::{
        mdbx::*,
        tables::{self, CallTraceSetEntry},
    },
    models::*,
    stagedsync::{stage::*, stages::*},
    stages::stage_util::*,
    StageId,
};
use anyhow::format_err;
use async_trait::async_trait;
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
                    flush_bitmap(&mut froms_collector, &mut froms);
                    flush_bitmap(&mut tos_collector, &mut tos);

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

        flush_bitmap(&mut froms_collector, &mut froms);
        flush_bitmap(&mut tos_collector, &mut tos);

        if printed {
            info!("Flushing froms index");
        }
        load_bitmap(&mut tx.cursor(tables::CallFromIndex)?, froms_collector)?;

        if printed {
            info!("Flushing tos index");
        }
        load_bitmap(&mut tx.cursor(tables::CallToIndex)?, tos_collector)?;

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

        unwind_bitmap(
            &mut tx.cursor(tables::CallFromIndex)?,
            from_addresses,
            input.unwind_to,
        )?;
        unwind_bitmap(
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

        prune_bitmap(
            &mut tx.cursor(tables::CallFromIndex)?,
            from_addresses,
            input.prune_to,
        )?;
        prune_bitmap(
            &mut tx.cursor(tables::CallToIndex)?,
            to_addresses,
            input.prune_to,
        )?;

        Ok(())
    }
}

// fn collect_bitmaps<'db, 'tx, E>(
//     tx: &'tx MdbxTransaction<'db, RW, E>,
//     temp_dir: &TempDir,
//     input: StageInput,
// ) -> anyhow::Result<ExecOutput>
// where
//     E: EnvironmentKind,
//     'db: 'tx,
// {
//     let starting_block = input.stage_progress.unwrap_or(BlockNumber(0));
//     let max_block = input
//         .previous_stage
//         .ok_or_else(|| format_err!("Call trace index generation cannot be the first stage"))?
//         .1;

//     let call_trace_set_cursor = tx.cursor(tables::CallTraceSet)?;
//     let walker = call_trace_set_cursor.walk(Some(starting_block + 1));
//     pin!(walker);

//     let mut froms = HashMap::<Address, croaring::Treemap>::new();
//     let mut tos = HashMap::<Address, croaring::Treemap>::new();

//     let mut froms_collector =
//         Collector::<Address, croaring::Treemap>::new(&*self.temp_dir, OPTIMAL_BUFFER_CAPACITY);
//     let mut tos_collector =
//         Collector::<Address, croaring::Treemap>::new(&*self.temp_dir, OPTIMAL_BUFFER_CAPACITY);

//     fn flush(
//         collector: &mut Collector<Address, croaring::Treemap>,
//         src: &mut HashMap<Address, croaring::Treemap>,
//     ) {
//         for (address, index) in src.drain() {
//             collector.push(address, index);
//         }
//     }

//     let mut highest_block = starting_block;
//     let mut last_flush = starting_block;

//     let mut printed = false;
//     let mut last_log = Instant::now();
//     while let Some((block_number, CallTraceSetEntry { address, from, to })) =
//         walker.next().transpose()?
//     {
//         if block_number > max_block {
//             break;
//         }

//         if from {
//             froms.entry(address).or_default().add(block_number.0);
//         }

//         if to {
//             tos.entry(address).or_default().add(block_number.0);
//         }

//         if highest_block != block_number {
//             highest_block = block_number;

//             if highest_block.0 - last_flush.0 >= self.flush_interval {
//                 flush(&mut froms_collector, &mut froms);
//                 flush(&mut tos_collector, &mut tos);

//                 last_flush = highest_block;
//             }
//         }

//         let now = Instant::now();
//         if last_log - now > Duration::from_secs(30) {
//             info!("Current block: {}", block_number);
//             printed = true;
//             last_log = now;
//         }
//     }

//     flush(&mut froms_collector, &mut froms);
//     flush(&mut tos_collector, &mut tos);

//     if printed {
//         info!("Flushing froms index");
//     }
//     load_bitmap(&mut tx.cursor(tables::CallFromIndex)?, froms_collector)?;

//     if printed {
//         info!("Flushing tos index");
//     }
//     load_bitmap(&mut tx.cursor(tables::CallToIndex)?, tos_collector)?;

//     Ok(ExecOutput::Progress {
//         stage_progress: max_block,
//         done: true,
//     })
// }

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bitmapdb;
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
