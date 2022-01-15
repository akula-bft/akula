use crate::{
    accessors,
    consensus::engine_factory,
    execution::{
        analysis_cache::AnalysisCache,
        processor::ExecutionProcessor,
        tracer::{CallTracer, CallTracerFlags},
    },
    h256_to_u256,
    kv::{
        tables::{self, CallTraceSetEntry},
        traits::*,
    },
    models::*,
    stagedsync::{format_duration, stage::*, stages::EXECUTION},
    upsert_storage_value, Buffer,
};
use anyhow::{format_err, Context};
use async_trait::async_trait;
use std::time::{Duration, Instant};
use tracing::*;

/// Execution of blocks through EVM
#[derive(Debug)]
pub struct Execution {
    pub batch_size: u64,
    pub history_batch_size: u64,
    pub exit_after_batch: bool,
    pub batch_until: Option<BlockNumber>,
    pub commit_every: Option<Duration>,
    pub prune_from: BlockNumber,
}

#[allow(clippy::too_many_arguments)]
async fn execute_batch_of_blocks<'db, Tx: MutableTransaction<'db>>(
    tx: &Tx,
    chain_config: ChainSpec,
    max_block: BlockNumber,
    batch_size: u64,
    history_batch_size: u64,
    batch_until: Option<BlockNumber>,
    commit_every: Option<Duration>,
    starting_block: BlockNumber,
    first_started_at: (Instant, Option<BlockNumber>),
    prune_from: BlockNumber,
) -> anyhow::Result<BlockNumber> {
    let mut buffer = Buffer::new(tx, prune_from, None);
    let mut consensus_engine = engine_factory(chain_config.clone())?;
    let mut analysis_cache = AnalysisCache::default();

    let mut block_number = starting_block;
    let mut gas_since_start = 0;
    let mut gas_since_last_message = 0;
    let mut gas_since_history_commit = 0;
    let batch_started_at = Instant::now();
    let first_started_at_gas = tx
        .get(
            tables::CumulativeIndex,
            first_started_at.1.unwrap_or(BlockNumber(0)),
        )
        .await?
        .unwrap()
        .gas;
    let mut last_message = Instant::now();
    let mut printed_at_least_once = false;
    loop {
        let block_hash = accessors::chain::canonical_hash::read(tx, block_number)
            .await?
            .ok_or_else(|| format_err!("No canonical hash found for block {}", block_number))?;
        let header = accessors::chain::header::read(tx, block_hash, block_number)
            .await?
            .ok_or_else(|| format_err!("Header not found: {}/{:?}", block_number, block_hash))?
            .into();
        let block = accessors::chain::block_body::read_with_senders(tx, block_hash, block_number)
            .await?
            .ok_or_else(|| {
                format_err!("Block body not found: {}/{:?}", block_number, block_hash)
            })?;

        let block_spec = chain_config.collect_block_spec(block_number);

        let mut call_tracer = CallTracer::default();
        let receipts = ExecutionProcessor::new(
            &mut buffer,
            Some(&mut call_tracer),
            &mut analysis_cache,
            &mut *consensus_engine,
            &header,
            &block,
            &block_spec,
        )
        .execute_and_write_block()
        .await
        .with_context(|| {
            format!(
                "Failed to execute block #{} ({:?})",
                block_number, block_hash
            )
        })?;

        buffer.insert_receipts(block_number, receipts);

        {
            let mut c = tx.mutable_cursor_dupsort(tables::CallTraceSet).await?;
            for (address, CallTracerFlags { from, to }) in call_tracer.into_sorted_iter() {
                c.append_dup(header.number, CallTraceSetEntry { address, from, to })
                    .await?;
            }
        }

        gas_since_start += header.gas_used;
        gas_since_last_message += header.gas_used;
        gas_since_history_commit += header.gas_used;

        if gas_since_history_commit >= history_batch_size {
            buffer.write_history().await?;
            gas_since_history_commit = 0;
        }

        let now = Instant::now();

        let stage_complete = block_number == max_block;

        let end_of_batch = stage_complete
            || block_number >= batch_until.unwrap_or(BlockNumber(u64::MAX))
            || gas_since_start >= batch_size
            || commit_every
                .map(|commit_every| now - batch_started_at > commit_every)
                .unwrap_or(false);

        let elapsed = now - last_message;
        if elapsed > Duration::from_secs(30) || (end_of_batch && !printed_at_least_once) {
            let current_total_gas = tx
                .get(tables::CumulativeIndex, block_number)
                .await?
                .unwrap()
                .gas;

            let total_gas = tx
                .cursor(tables::CumulativeIndex)
                .await?
                .last()
                .await?
                .unwrap()
                .1
                .gas;
            let mgas_sec = gas_since_last_message as f64
                / (elapsed.as_secs() as f64 + (elapsed.subsec_millis() as f64 / 1000_f64))
                / 1_000_000f64;
            info!(
                "Executed block {}, Mgas/sec: {:.2}{}",
                block_number,
                mgas_sec,
                if stage_complete {
                    String::new()
                } else {
                    let elapsed_since_start = now - first_started_at.0;
                    let ratio_complete = (current_total_gas - first_started_at_gas) as f64
                        / (total_gas - first_started_at_gas) as f64;

                    let estimated_total_time = Duration::from_secs(
                        (elapsed_since_start.as_secs() as f64 / ratio_complete) as u64,
                    );

                    debug!(
                        "Elapsed since start {:?}, ratio complete {:?}, estimated total time {:?}",
                        elapsed_since_start, ratio_complete, estimated_total_time
                    );

                    format!(
                        ", progress: {:0>2.2}%, {} remaining",
                        ratio_complete * 100_f64,
                        format_duration(
                            estimated_total_time.saturating_sub(elapsed_since_start),
                            false
                        )
                    )
                }
            );
            printed_at_least_once = true;
            last_message = now;
            gas_since_last_message = 0;
        }

        if end_of_batch {
            break;
        }

        block_number.0 += 1;
    }

    buffer.write_to_db().await?;

    Ok(block_number)
}

#[async_trait]
impl<'db, RwTx: MutableTransaction<'db>> Stage<'db, RwTx> for Execution {
    fn id(&self) -> crate::StageId {
        EXECUTION
    }

    async fn execute<'tx>(
        &mut self,
        tx: &'tx mut RwTx,
        input: StageInput,
    ) -> anyhow::Result<ExecOutput>
    where
        'db: 'tx,
    {
        let _ = tx;

        let genesis_hash = tx
            .get(tables::CanonicalHeader, BlockNumber(0))
            .await?
            .ok_or_else(|| format_err!("Genesis block absent"))?;
        let chain_config = tx
            .get(tables::Config, genesis_hash)
            .await?
            .ok_or_else(|| format_err!("No chain config for genesis block {:?}", genesis_hash))?;

        let prev_progress = input.stage_progress.unwrap_or_default();
        let starting_block = prev_progress + 1;
        let max_block = input
            .previous_stage.ok_or_else(|| format_err!("Execution stage cannot be executed first, but no previous stage progress specified"))?.1;

        Ok(if max_block >= starting_block {
            let executed_to = execute_batch_of_blocks(
                tx,
                chain_config,
                max_block,
                self.batch_size,
                self.history_batch_size,
                self.batch_until,
                self.commit_every,
                starting_block,
                input.first_started_at,
                self.prune_from,
            )
            .await?;

            let done = executed_to == max_block || self.exit_after_batch;

            ExecOutput::Progress {
                stage_progress: executed_to,
                done,
            }
        } else {
            ExecOutput::Progress {
                stage_progress: prev_progress,
                done: true,
            }
        })
    }

    async fn unwind<'tx>(
        &mut self,
        tx: &'tx mut RwTx,
        input: crate::stagedsync::stage::UnwindInput,
    ) -> anyhow::Result<UnwindOutput>
    where
        'db: 'tx,
    {
        info!("Unwinding accounts");
        let mut account_cursor = tx.mutable_cursor(tables::Account).await?;

        let mut account_cs_cursor = tx.mutable_cursor(tables::AccountChangeSet).await?;

        while let Some((block_number, tables::AccountChange { address, account })) =
            account_cs_cursor.last().await?
        {
            if block_number <= input.unwind_to {
                break;
            }

            if let Some(account) = account {
                account_cursor.put(address, account).await?;
            } else if account_cursor.seek(address).await?.is_some() {
                account_cursor.delete_current().await?;
            }

            account_cs_cursor.delete_current().await?;
        }

        info!("Unwinding storage");
        let mut storage_cursor = tx.mutable_cursor_dupsort(tables::Storage).await?;

        let mut storage_cs_cursor = tx.mutable_cursor_dupsort(tables::StorageChangeSet).await?;

        while let Some((
            tables::StorageChangeKey {
                block_number,
                address,
            },
            tables::StorageChange { location, value },
        )) = storage_cs_cursor.last().await?
        {
            if block_number <= input.unwind_to {
                break;
            }

            upsert_storage_value(&mut storage_cursor, address, h256_to_u256(location), value)
                .await?;

            storage_cs_cursor.delete_current().await?;
        }

        info!("Unwinding logs");
        let mut log_cursor = tx.mutable_cursor(tables::Log).await?;
        while let Some(((block_number, _), _)) = log_cursor.last().await? {
            if block_number <= input.unwind_to {
                break;
            }

            log_cursor.delete_current().await?;
        }

        info!("Unwinding call trace sets");
        let mut call_trace_set_cursor = tx.mutable_cursor_dupsort(tables::CallTraceSet).await?;
        while let Some((block_number, _)) = call_trace_set_cursor.last().await? {
            if block_number <= input.unwind_to {
                break;
            }

            call_trace_set_cursor.delete_current_duplicates().await?;
        }

        Ok(UnwindOutput {
            stage_progress: input.unwind_to,
        })
    }
}
