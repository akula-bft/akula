use crate::{
    bitmapdb::{self, CHUNK_LIMIT},
    etl::collector::*,
    kv::{
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
};
use tempfile::TempDir;
use tokio::pin;
use tokio_stream::StreamExt;

/// Generate call trace index
#[derive(Debug)]
pub struct CallTraceIndex {
    pub temp_dir: Arc<TempDir>,
    pub flush_interval: u64,
}

#[async_trait]
impl<'db, RwTx> Stage<'db, RwTx> for CallTraceIndex
where
    RwTx: MutableTransaction<'db>,
{
    fn id(&self) -> StageId {
        CALL_TRACES
    }

    async fn execute<'tx>(
        &mut self,
        tx: &'tx mut RwTx,
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

        let mut call_trace_set_cursor = tx.cursor_dup_sort(tables::CallTraceSet).await?;
        let walker = walk(&mut call_trace_set_cursor, Some(starting_block + 1));
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
        while let Some((block_number, CallTraceSetEntry { address, from, to })) =
            walker.try_next().await?
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
        }

        flush(&mut froms_collector, &mut froms);
        flush(&mut tos_collector, &mut tos);

        load_call_traces(
            &mut tx.mutable_cursor(tables::CallFromIndex).await?,
            froms_collector,
        )
        .await?;
        load_call_traces(
            &mut tx.mutable_cursor(tables::CallToIndex).await?,
            tos_collector,
        )
        .await?;

        Ok(ExecOutput::Progress {
            stage_progress: max_block,
            done: true,
        })
    }

    async fn unwind<'tx>(
        &mut self,
        tx: &'tx mut RwTx,
        input: UnwindInput,
    ) -> anyhow::Result<UnwindOutput>
    where
        'db: 'tx,
    {
        let mut call_trace_set_cursor = tx.mutable_cursor_dupsort(tables::CallTraceSet).await?;

        let mut to_addresses = BTreeSet::<Address>::new();
        let mut from_addresses = BTreeSet::<Address>::new();

        let walker = walk(&mut call_trace_set_cursor, Some(input.unwind_to + 1));
        pin!(walker);
        while let Some((_, entry)) = walker.try_next().await? {
            if entry.to {
                to_addresses.insert(entry.address);
            }

            if entry.from {
                from_addresses.insert(entry.address);
            }
        }

        unwind_call_traces(
            &mut tx.mutable_cursor(tables::CallFromIndex).await?,
            from_addresses,
            input.unwind_to,
        )
        .await?;
        unwind_call_traces(
            &mut tx.mutable_cursor(tables::CallToIndex).await?,
            to_addresses,
            input.unwind_to,
        )
        .await?;

        Ok(UnwindOutput {
            stage_progress: input.unwind_to,
        })
    }
}

async fn load_call_traces<'tx, 'tmp, C, T>(
    cursor: &mut C,
    mut collector: Collector<'tmp, Address, croaring::Treemap>,
) -> anyhow::Result<()>
where
    C: MutableCursor<'tx, T>,
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
            if let Some((_, last_bitmap)) = cursor
                .seek_exact(BitmapKey {
                    inner: address,
                    block_number: BlockNumber(u64::MAX),
                })
                .await?
            {
                total_bitmap |= last_bitmap;
            }

            for (block_number, bitmap) in
                bitmapdb::Chunks::new(total_bitmap, CHUNK_LIMIT).with_keys()
            {
                cursor
                    .put(
                        BitmapKey {
                            inner: address,
                            block_number,
                        },
                        bitmap,
                    )
                    .await?;
            }
        }
    }

    Ok(())
}

async fn unwind_call_traces<'tx, C, T>(
    cursor: &mut C,
    addresses: BTreeSet<Address>,
    unwind_to: BlockNumber,
) -> anyhow::Result<()>
where
    C: MutableCursor<'tx, T>,
    T: Table<Key = BitmapKey<Address>, Value = croaring::Treemap>,
{
    for address in addresses {
        let mut bm = cursor
            .seek_exact(BitmapKey {
                inner: address,
                block_number: BlockNumber(u64::MAX),
            })
            .await?
            .map(|(_, bm)| bm);

        while let Some(b) = bm {
            cursor.delete_current().await?;

            let new_bm = b
                .iter()
                .take_while(|&v| v <= *unwind_to)
                .collect::<croaring::Treemap>();

            if new_bm.cardinality() > 0 {
                cursor
                    .upsert(
                        BitmapKey {
                            inner: address,
                            block_number: BlockNumber(u64::MAX),
                        },
                        new_bm,
                    )
                    .await?;
            }

            bm = cursor.prev().await?.and_then(
                |(BitmapKey { inner, .. }, b)| if inner == address { Some(b) } else { None },
            );
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use super::*;

    #[tokio::test]
    async fn call_traces() {
        let db = crate::kv::new_mem_database().unwrap();

        let mut tx = db.begin_mutable().await.unwrap();

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
            .await
            .unwrap();
        }

        let mut address = Address::zero();
        address.0[19] = 1;

        async fn froms<'db, Tx: MutableTransaction<'db>>(
            tx: &Tx,
            address: Address,
        ) -> croaring::Treemap {
            bitmapdb::get(
                tx,
                tables::CallFromIndex,
                address,
                BlockNumber(0)..=BlockNumber(30),
            )
            .await
            .unwrap()
        }

        async fn tos<'db, Tx: MutableTransaction<'db>>(
            tx: &Tx,
            address: Address,
        ) -> croaring::Treemap {
            bitmapdb::get(
                tx,
                tables::CallToIndex,
                address,
                BlockNumber(0)..=BlockNumber(30),
            )
            .await
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
            (froms)(&tx, address).await.iter().collect::<Vec<_>>()
        );
        assert_eq!(
            vec![1, 11],
            (tos)(&tx, address).await.iter().collect::<Vec<_>>()
        );

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

        assert_eq!(
            vec![6],
            (froms)(&tx, address).await.iter().collect::<Vec<_>>()
        );
        assert_eq!(
            vec![1],
            (tos)(&tx, address).await.iter().collect::<Vec<_>>()
        );

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
            (froms)(&tx, address).await.iter().collect::<Vec<_>>()
        );
        assert_eq!(
            vec![1, 11, 21],
            (tos)(&tx, address).await.iter().collect::<Vec<_>>()
        );
    }
}
