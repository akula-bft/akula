use crate::{
    accessors,
    consensus::{engine_factory, CliqueError, ConsensusState, DuoError, ValidationError, is_parlia, DIFF_INTURN},
    execution::{
        analysis_cache::AnalysisCache,
        processor::ExecutionProcessor,
        tracer::{CallTracer, CallTracerFlags},
    },
    h256_to_u256,
    kv::{
        mdbx::*,
        tables::{self, CallTraceSetEntry},
    },
    models::*,
    stagedsync::{format_duration, stage::*, util::*},
    upsert_storage_value, Buffer, StageId,
};
use anyhow::format_err;
use async_trait::async_trait;
use std::time::{Duration, Instant};
use tracing::*;

pub const EXECUTION: StageId = StageId("Execution");

/// Execution of blocks through EVM
#[derive(Debug)]
pub struct Execution {
    pub max_block: Option<BlockNumber>,
    pub batch_size: u64,
    pub history_batch_size: u64,
    pub exit_after_batch: bool,
    pub batch_until: Option<BlockNumber>,
    pub commit_every: Option<Duration>,
}

#[allow(clippy::too_many_arguments)]
fn execute_batch_of_blocks<E: EnvironmentKind>(
    tx: &MdbxTransaction<'_, RW, E>,
    chain_config: ChainSpec,
    max_block: BlockNumber,
    batch_size: u64,
    history_batch_size: u64,
    batch_until: Option<BlockNumber>,
    commit_every: Option<Duration>,
    starting_block: BlockNumber,
    first_started_at: (Instant, Option<BlockNumber>),
) -> Result<BlockNumber, StageError> {
    let mut consensus_engine = engine_factory(None, chain_config.clone(), None)?;
    consensus_engine.set_state(ConsensusState::recover(tx, &chain_config, starting_block)?);

    let mut buffer = Buffer::new(tx, None);
    let mut analysis_cache = AnalysisCache::default();

    let mut block_number = starting_block;
    let mut gas_since_start = 0;
    let mut gas_since_last_message = 0;
    let mut gas_since_history_commit = 0;
    let batch_started_at = Instant::now();
    let first_started_at_gas = tx
        .get(
            tables::TotalGas,
            first_started_at.1.unwrap_or(BlockNumber(0)),
        )?
        .unwrap();
    let mut last_message = Instant::now();
    let mut printed_at_least_once = false;
    loop {
        let block_hash = tx
            .get(tables::CanonicalHeader, block_number)?
            .ok_or_else(|| format_err!("No canonical hash found for block {}", block_number))?;
        let header = tx
            .get(tables::Header, (block_number, block_hash))?
            .ok_or_else(|| format_err!("Header not found: {}/{:?}", block_number, block_hash))?;
        let block = accessors::chain::block_body::read_with_senders(tx, block_hash, block_number)?
            .ok_or_else(|| {
                format_err!("Block body not found: {}/{:?}", block_number, block_hash)
            })?;

        let block_spec = chain_config.collect_block_spec(block_number);

        if !consensus_engine.is_state_valid(&header) {
            consensus_engine.set_state(ConsensusState::recover(tx, &chain_config, block_number)?);
        }

        if is_parlia(consensus_engine.name()) && header.difficulty != DIFF_INTURN {
            consensus_engine.snapshot(tx, BlockNumber(header.number.0-1), header.parent_hash)?;
        }

        let mut call_tracer = CallTracer::default();
        let receipts = ExecutionProcessor::new(
            &mut buffer,
            &mut call_tracer,
            &mut analysis_cache,
            &mut *consensus_engine,
            &header,
            &block,
            &block_spec,
            &chain_config,
        )
        .execute_and_write_block()
        .map_err(|e| match e {
            DuoError::Validation(error) => StageError::Validation {
                block: block_number,
                error,
            },
            DuoError::Internal(e) => StageError::Internal(e.context(format!(
                "Failed to execute block #{} ({:?})",
                block_number, block_hash
            ))),
        })?;

        buffer.insert_receipts(block_number, receipts);

        {
            let mut c = tx.cursor(tables::CallTraceSet)?;
            for (address, CallTracerFlags { from, to }) in call_tracer.into_sorted_iter() {
                c.append_dup(header.number, CallTraceSetEntry { address, from, to })?;
            }
        }

        gas_since_start += header.gas_used;
        gas_since_last_message += header.gas_used;
        gas_since_history_commit += header.gas_used;

        if gas_since_history_commit >= history_batch_size {
            buffer.write_history()?;
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
            let current_total_gas = tx.get(tables::TotalGas, block_number)?.unwrap();

            let total_gas = tx.cursor(tables::TotalGas)?.last()?.unwrap().1;
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

    buffer.write_to_db()?;

    Ok(block_number)
}

#[async_trait]
impl<'db, E> Stage<'db, E> for Execution
where
    E: EnvironmentKind,
{
    fn id(&self) -> crate::StageId {
        EXECUTION
    }

    async fn execute<'tx>(
        &mut self,
        tx: &'tx mut MdbxTransaction<'db, RW, E>,
        input: StageInput,
    ) -> Result<ExecOutput, StageError>
    where
        'db: 'tx,
    {
        let chain_config = tx
            .get(tables::Config, ())?
            .ok_or_else(|| format_err!("No chain specification set"))?;

        let prev_progress = input.stage_progress.unwrap_or_default();
        let starting_block = prev_progress + 1;
        let max_block = std::cmp::min(input
            .previous_stage.ok_or_else(|| format_err!("Execution stage cannot be executed first, but no previous stage progress specified"))?.1, self.max_block.unwrap_or(BlockNumber(u64::MAX)));

        Ok(if max_block >= starting_block {
            let result = execute_batch_of_blocks(
                tx,
                chain_config,
                max_block,
                self.batch_size,
                self.history_batch_size,
                self.batch_until,
                self.commit_every,
                starting_block,
                input.first_started_at,
            );

            // CliqueError::SignedRecently seems to be raised sometimes when
            // a reorg is in process. This is a kludge to recover in such cases.
            if let Err(StageError::Internal(ref e)) = result {
                for cause in e.chain() {
                    if let Some(DuoError::Validation(ValidationError::CliqueError(
                        CliqueError::SignedRecently {
                            signer: _,
                            current,
                            last: _,
                            limit,
                        },
                    ))) = cause.downcast_ref::<DuoError>()
                    {
                        let unwind_to = current.saturating_sub(*limit).into();
                        warn!(
                            "Trying to recover from {} by unwinding to {}",
                            &cause, unwind_to
                        );
                        return Ok(ExecOutput::Unwind { unwind_to });
                    }
                }
            }

            let executed_to = result?;

            let done = executed_to == max_block || self.exit_after_batch;

            ExecOutput::Progress {
                stage_progress: executed_to,
                done,
                reached_tip: true,
            }
        } else {
            ExecOutput::Progress {
                stage_progress: prev_progress,
                done: true,
                reached_tip: true,
            }
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
        info!("Unwinding accounts");
        let mut account_cursor = tx.cursor(tables::Account)?;

        let mut account_cs_cursor = tx.cursor(tables::AccountChangeSet)?;

        while let Some((block_number, tables::AccountChange { address, account })) =
            account_cs_cursor.last()?
        {
            if block_number <= input.unwind_to {
                break;
            }

            if let Some(account) = account {
                account_cursor.put(address, account)?;
            } else if account_cursor.seek_exact(address)?.is_some() {
                account_cursor.delete_current()?;
            }

            account_cs_cursor.delete_current()?;
        }

        info!("Unwinding storage");
        let mut storage_cursor = tx.cursor(tables::Storage)?;

        let mut storage_cs_cursor = tx.cursor(tables::StorageChangeSet)?;

        while let Some((
            tables::StorageChangeKey {
                block_number,
                address,
            },
            tables::StorageChange { location, value },
        )) = storage_cs_cursor.last()?
        {
            if block_number <= input.unwind_to {
                break;
            }

            upsert_storage_value(&mut storage_cursor, address, h256_to_u256(location), value)?;

            storage_cs_cursor.delete_current()?;
        }

        info!("Unwinding log indexes");
        unwind_by_block_key_duplicates(
            tx,
            tables::LogTopicsByBlock,
            input,
            std::convert::identity,
        )?;
        unwind_by_block_key_duplicates(
            tx,
            tables::LogAddressesByBlock,
            input,
            std::convert::identity,
        )?;

        info!("Unwinding call trace sets");
        unwind_by_block_key_duplicates(tx, tables::CallTraceSet, input, std::convert::identity)?;

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
        prune_by_block_key(tx, tables::AccountChangeSet, input, std::convert::identity)?;
        prune_by_block_key(
            tx,
            tables::StorageChangeSet,
            input,
            |tables::StorageChangeKey { block_number, .. }| block_number,
        )?;
        prune_by_block_key_duplicates(
            tx,
            tables::LogAddressesByBlock,
            input,
            std::convert::identity,
        )?;
        prune_by_block_key_duplicates(tx, tables::LogTopicsByBlock, input, std::convert::identity)?;
        prune_by_block_key_duplicates(tx, tables::CallTraceSet, input, std::convert::identity)?;

        Ok(())
    }
}
