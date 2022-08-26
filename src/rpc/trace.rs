use super::helpers;
use crate::{
    bitmapdb,
    consensus::engine_factory,
    execution::{
        analysis_cache::AnalysisCache, processor::execute_transaction, tracer::adhoc::AdhocTracer,
    },
    kv::{mdbx::*, tables, MdbxWithDirHandle},
    models::*,
    u256_to_h256, Buffer, HeaderReader, IntraBlockState, StateReader, StateWriter,
};
use anyhow::format_err;
use async_trait::async_trait;
use bytes::Bytes;
use ethereum_interfaces::web3::{
    self, trace_api_server::TraceApi, CallRequest, CallRequests, FilterRequest, FullTrace,
    FullTraces, OptionalFullTracesWithTransactionHashes, OptionalTracesWithLocation,
    TraceBlockRequest, TraceTransactionRequest, TraceWithLocation, TracesWithLocation,
};
use ethereum_jsonrpc::{
    types::{self, TransactionTraceWithLocation},
    TraceApiServer, TraceFilterMode,
};
use futures::stream::BoxStream;
use jsonrpsee::core::{Error as RpcError, RpcResult};
use maplit::hashset;
use std::{
    collections::{BTreeSet, HashSet},
    iter::once,
    sync::Arc,
};
use tokio_stream::{Stream, StreamExt};
use tonic::Response;
use tracing::*;
use crate::consensus::is_parlia;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StateUpdate {
    EraseStorage(Address),
    Storage {
        address: Address,
        location: U256,
        initial: U256,
        current: U256,
    },
    Account {
        address: Address,
        initial: Option<Account>,
        current: Option<Account>,
    },
    Code {
        code_hash: H256,
        code: Bytes,
    },
}

#[derive(Debug)]
struct LoggingBuffer<'buffer, 'db, 'tx, K, E>
where
    'db: 'tx,
    'db: 'buffer,
    'tx: 'buffer,
    K: TransactionKind,
    E: EnvironmentKind,
{
    inner: &'buffer mut Buffer<'db, 'tx, K, E>,
    updates: Vec<StateUpdate>,
}

impl<'buffer, 'db, 'tx, K, E> LoggingBuffer<'buffer, 'db, 'tx, K, E>
where
    'db: 'tx,
    'db: 'buffer,
    'tx: 'buffer,
    K: TransactionKind,
    E: EnvironmentKind,
{
    pub fn new(buffer: &'buffer mut Buffer<'db, 'tx, K, E>) -> Self {
        Self {
            inner: buffer,
            updates: Vec::new(),
        }
    }

    pub fn into_updates(self) -> Vec<StateUpdate> {
        self.updates
    }
}

impl<'buffer, 'db, 'tx, K, E> HeaderReader for LoggingBuffer<'buffer, 'db, 'tx, K, E>
where
    'db: 'tx,
    'db: 'buffer,
    'tx: 'buffer,
    K: TransactionKind,
    E: EnvironmentKind,
{
    fn read_header(
        &self,
        block_number: BlockNumber,
        block_hash: H256,
    ) -> anyhow::Result<Option<BlockHeader>> {
        self.inner.read_header(block_number, block_hash)
    }
}

impl<'buffer, 'db, 'tx, K, E> StateReader for LoggingBuffer<'buffer, 'db, 'tx, K, E>
where
    'db: 'tx,
    'db: 'buffer,
    'tx: 'buffer,
    K: TransactionKind,
    E: EnvironmentKind,
{
    fn read_account(&self, address: Address) -> anyhow::Result<Option<Account>> {
        self.inner.read_account(address)
    }

    fn read_code(&self, code_hash: H256) -> anyhow::Result<Bytes> {
        self.inner.read_code(code_hash)
    }

    fn read_storage(&self, address: Address, location: U256) -> anyhow::Result<U256> {
        self.inner.read_storage(address, location)
    }
}

impl<'buffer, 'db, 'tx, K, E> StateWriter for LoggingBuffer<'buffer, 'db, 'tx, K, E>
where
    'db: 'tx,
    'db: 'buffer,
    'tx: 'buffer,
    K: TransactionKind,
    E: EnvironmentKind,
{
    fn erase_storage(&mut self, address: Address) -> anyhow::Result<()> {
        self.updates.push(StateUpdate::EraseStorage(address));
        self.inner.erase_storage(address)
    }

    fn begin_block(&mut self, block_number: BlockNumber) {
        self.inner.begin_block(block_number)
    }

    fn update_account(
        &mut self,
        address: Address,
        initial: Option<Account>,
        current: Option<Account>,
    ) {
        self.updates.push(StateUpdate::Account {
            address,
            initial,
            current,
        });
        self.inner.update_account(address, initial, current)
    }

    fn update_code(&mut self, code_hash: H256, code: Bytes) -> anyhow::Result<()> {
        self.updates.push(StateUpdate::Code {
            code_hash,
            code: code.clone(),
        });
        self.inner.update_code(code_hash, code)
    }

    fn update_storage(
        &mut self,
        address: Address,
        location: U256,
        initial: U256,
        current: U256,
    ) -> anyhow::Result<()> {
        self.updates.push(StateUpdate::Storage {
            address,
            location,
            initial,
            current,
        });
        self.inner
            .update_storage(address, location, initial, current)
    }
}

pub struct TraceApiServerImpl<SE>
where
    SE: EnvironmentKind,
{
    pub db: Arc<MdbxWithDirHandle<SE>>,
    pub call_gas_limit: u64,
}

#[derive(Debug)]
enum CallManyMode {
    /// Re-execute existing transactions belonging to this block
    Replay(types::BlockId),
    /// Speculative execution based on state of this block
    Speculative(types::BlockId),
}

fn do_call_many<K, E>(
    txn: &MdbxTransaction<'_, K, E>,
    kind: CallManyMode,
    calls: Vec<(Address, Message, HashSet<types::TraceType>)>,
    finalization: Option<Vec<BlockHeader>>,
) -> anyhow::Result<(Vec<types::FullTrace>, Vec<types::RewardAction>)>
where
    K: TransactionKind,
    E: EnvironmentKind,
{
    let mut traces = Vec::with_capacity(calls.len());

    let chain_spec = txn
        .get(tables::Config, ())?
        .ok_or_else(|| format_err!("chain spec not found"))?;

    let (historical_block, block_number, header) = match kind {
        CallManyMode::Replay(b) => {
            let (block_number, block_hash) =
                helpers::resolve_block_id(txn, b)?.ok_or_else(|| format_err!("block not found"))?;
            (
                Some(
                    block_number
                        .checked_sub(1)
                        .ok_or_else(|| format_err!("cannot replay genesis block"))?
                        .into(),
                ),
                block_number,
                crate::accessors::chain::header::read(txn, block_hash, block_number)?
                    .ok_or_else(|| format_err!("header not found"))?,
            )
        }
        CallManyMode::Speculative(b) => match b {
            types::BlockId::Number(types::BlockNumber::Latest)
            | types::BlockId::Number(types::BlockNumber::Pending) => {
                let (latest_block_number, latest_block_hash) =
                    helpers::resolve_block_id(txn, b)?
                        .ok_or_else(|| format_err!("block not found"))?;

                let header = crate::accessors::chain::header::read(
                    txn,
                    latest_block_hash,
                    latest_block_number,
                )?
                .ok_or_else(|| format_err!("header not found"))?;
                (None, latest_block_number, header)
            }
            other => {
                let (block_number, block_hash) = helpers::resolve_block_id(txn, other)?
                    .ok_or_else(|| format_err!("block not found"))?;

                let header = crate::accessors::chain::header::read(txn, block_hash, block_number)?
                    .ok_or_else(|| format_err!("header not found"))?;

                (Some(block_number), block_number, header)
            }
        },
    };

    trace!("Replaying {} calls on top of historical block {historical_block:?}, of block {block_number:?} with header {header:?}", calls.len());

    let block_spec = chain_spec.collect_block_spec(block_number);
    let mut buffer = Buffer::new(txn, historical_block);

    let engine = engine_factory(None, chain_spec.clone(), None)?;

    let mut analysis_cache = AnalysisCache::default();
    for (sender, message, trace_types) in calls {
        let (output, updates, trace) = {
            let mut buffer = LoggingBuffer::new(&mut buffer);
            let mut state = IntraBlockState::new(&mut buffer);

            let mut tracer = AdhocTracer::new();

            let mut gas_used = 0;
            let (output, _) = execute_transaction(
                &mut state,
                &block_spec,
                &header,
                &mut tracer,
                &mut analysis_cache,
                &mut gas_used,
                &message,
                sender,
                engine.get_beneficiary(&header),
                is_parlia(engine.name()),
            )?;

            state.write_to_state_same_block()?;
            (
                output,
                buffer.into_updates(),
                if trace_types.contains(&types::TraceType::Trace) {
                    Some(tracer.into_trace())
                } else {
                    None
                },
            )
        };

        let mut state_diff = if trace_types.contains(&types::TraceType::StateDiff) {
            Some(types::StateDiff(Default::default()))
        } else {
            None
        };

        if let Some(types::StateDiff(state_diff)) = state_diff.as_mut() {
            for update in updates {
                match &update {
                    StateUpdate::Storage {
                        address,
                        location,
                        initial,
                        current,
                    } => {
                        if initial != current {
                            state_diff.entry(*address).or_default().storage.insert(
                                u256_to_h256(*location),
                                if *initial > 0 {
                                    types::Delta::Altered(types::AlteredType {
                                        from: u256_to_h256(*initial),
                                        to: u256_to_h256(*current),
                                    })
                                } else {
                                    types::Delta::Added(u256_to_h256(*current))
                                },
                            );
                        }
                    }
                    StateUpdate::Account {
                        address,
                        initial,
                        current,
                    } => {
                        if *initial != *current {
                            match (initial, current) {
                                (None, Some(account)) => {
                                    let diff = state_diff.entry(*address).or_insert_with(|| {
                                        types::AccountDiff {
                                            balance: types::Delta::Unchanged,
                                            nonce: types::Delta::Unchanged,
                                            code: types::Delta::Unchanged,
                                            storage: Default::default(),
                                        }
                                    });
                                    diff.balance = types::Delta::Added(account.balance);
                                    diff.nonce = types::Delta::Added(account.nonce.into());
                                    diff.code = types::Delta::Added(
                                        buffer.read_code(account.code_hash)?.into(),
                                    );
                                }
                                (Some(initial), None) => {
                                    let diff = state_diff.entry(*address).or_default();
                                    diff.balance = types::Delta::Removed(initial.balance);
                                    diff.nonce = types::Delta::Removed(initial.nonce.into());
                                    diff.code = types::Delta::Removed(
                                        buffer.read_code(initial.code_hash)?.into(),
                                    );
                                }
                                (Some(initial), Some(current)) => {
                                    let diff = state_diff.entry(*address).or_default();

                                    fn make_delta<T: PartialEq>(from: T, to: T) -> types::Delta<T> {
                                        if from == to {
                                            types::Delta::Unchanged
                                        } else {
                                            types::Delta::Altered(types::AlteredType { from, to })
                                        }
                                    }
                                    diff.balance = make_delta(initial.balance, current.balance);
                                    diff.nonce =
                                        make_delta(initial.nonce.into(), current.nonce.into());
                                    diff.code = make_delta(
                                        buffer.read_code(initial.code_hash)?.into(),
                                        buffer.read_code(current.code_hash)?.into(),
                                    );
                                }
                                _ => unreachable!(),
                            }
                        }
                    }
                    _ => {}
                }
            }
        }

        traces.push(types::FullTrace {
            output: output.into(),
            trace,
            // todo
            vm_trace: None,
            state_diff,
        })
    }

    let mut rewards = vec![];
    if let Some(ommers) = finalization {
        for change in engine_factory(None, chain_spec, None)?.finalize(&header, &ommers, None, &buffer)? {
            match change {
                crate::consensus::FinalizationChange::Reward {
                    address,
                    amount,
                    ommer,
                } => rewards.push(types::RewardAction {
                    author: address,
                    value: amount,
                    reward_type: if ommer {
                        types::RewardType::Uncle
                    } else {
                        types::RewardType::Block
                    },
                }),
            }
        }
    }

    Ok((traces, rewards))
}

fn replay_block_transactions<K, E>(
    txn: &MdbxTransaction<'_, K, E>,
    block_id: types::BlockId,
    trace_types: HashSet<types::TraceType>,
    finalize: bool,
) -> RpcResult<
    Option<(
        BlockNumber,
        H256,
        Vec<types::FullTraceWithTransactionHash>,
        Vec<types::RewardAction>,
    )>,
>
where
    K: TransactionKind,
    E: EnvironmentKind,
{
    Ok(
        if let Some((block_number, block_hash)) = helpers::resolve_block_id(txn, block_id)? {
            let BlockBody {
                transactions,
                ommers,
            } = crate::accessors::chain::block_body::read_without_senders(
                txn,
                block_hash,
                block_number,
            )?
            .ok_or_else(|| format_err!("body not found for block #{block_number}/{block_hash}"))?;
            let senders = crate::accessors::chain::tx_sender::read(txn, block_hash, block_number)?;

            let signed_messages = transactions;

            let hashes = signed_messages
                .iter()
                .map(|signed_message| signed_message.hash())
                .collect::<Vec<_>>();

            let (traces, rewards) = do_call_many(
                txn,
                CallManyMode::Replay(block_id),
                signed_messages
                    .into_iter()
                    .zip(senders)
                    .map(|(signed_message, sender)| {
                        (sender, signed_message.message, trace_types.clone())
                    })
                    .collect(),
                if finalize { Some(ommers) } else { None },
            )?;

            Some((
                block_number,
                block_hash,
                traces
                    .into_iter()
                    .zip(hashes)
                    .map(
                        |(full_trace, transaction_hash)| types::FullTraceWithTransactionHash {
                            full_trace,
                            transaction_hash,
                        },
                    )
                    .collect(),
                rewards,
            ))
        } else {
            None
        },
    )
}

fn replay_block<K, E>(
    txn: &MdbxTransaction<'_, K, E>,
    block_id: types::BlockId,
) -> RpcResult<Option<(BlockNumber, H256, Vec<TransactionTraceWithLocation>)>>
where
    K: TransactionKind,
    E: EnvironmentKind,
{
    Ok(
        replay_block_transactions(txn, block_id, hashset![types::TraceType::Trace], true)?.map(
            |(block_number, block_hash, full_traces, rewards)| {
                (
                    block_number,
                    block_hash,
                    full_traces
                        .into_iter()
                        .enumerate()
                        .flat_map(
                            |(
                                transaction_position,
                                types::FullTraceWithTransactionHash {
                                    full_trace,
                                    transaction_hash,
                                },
                            )| {
                                full_trace
                                    .trace
                                    .unwrap_or_default()
                                    .into_iter()
                                    .map(move |trace| TransactionTraceWithLocation {
                                        trace,
                                        transaction_position: Some(transaction_position),
                                        transaction_hash: Some(transaction_hash),
                                        block_number: block_number.0.into(),
                                        block_hash,
                                    })
                            },
                        )
                        .chain(
                            rewards
                                .into_iter()
                                .map(|reward| TransactionTraceWithLocation {
                                    trace: types::TransactionTrace {
                                        trace_address: vec![],
                                        subtraces: 0,
                                        action: types::Action::Reward(reward),
                                        result: None,
                                    },
                                    transaction_position: None,
                                    transaction_hash: None,
                                    block_number: block_number.0.into(),
                                    block_hash,
                                }),
                        )
                        .collect(),
                )
            },
        ),
    )
}

impl<DB> TraceApiServerImpl<DB>
where
    DB: EnvironmentKind,
{
    pub fn filter_stream(
        &self,
        filter: ethereum_jsonrpc::Filter,
    ) -> impl Stream<Item = anyhow::Result<types::TransactionTraceWithLocation>> {
        let ethereum_jsonrpc::Filter {
            from_block,
            to_block,
            from_address,
            to_address,
            mode,
            ..
        } = filter;

        let from_block = from_block.unwrap_or(types::BlockId::Number(types::BlockNumber::Earliest));
        let to_block = to_block.unwrap_or(types::BlockId::Number(types::BlockNumber::Latest));

        let db = self.db.clone();

        let (res_tx, rx) = tokio::sync::mpsc::channel(1);

        tokio::task::spawn_blocking(move || {
            let f = {
                let res_tx = res_tx.clone();
                move || {
                    let txn = db.begin()?;

                    let (from_block, _) = helpers::resolve_block_id(&txn, from_block)?
                        .ok_or_else(|| format_err!("invalid from block"))?;

                    let (to_block, _) = helpers::resolve_block_id(&txn, to_block)?
                        .ok_or_else(|| format_err!("invalid from block"))?;

                    if from_block > to_block {
                        return Err(format_err!(
                            "from_block higher than to_block: {from_block} > {to_block}"
                        ));
                    }

                    let requested_from_addresses = from_address.unwrap_or_default();
                    let requested_to_addresses = to_address.unwrap_or_default();
                    let mode = mode.unwrap_or(TraceFilterMode::Union);

                    let mut from_addresses = HashSet::with_capacity(requested_from_addresses.len());
                    let mut to_addresses = HashSet::with_capacity(requested_to_addresses.len());

                    let mut blocks_to_scan = croaring::Treemap::default();

                    for address in &requested_from_addresses {
                        let bitmap = bitmapdb::get(
                            &txn,
                            tables::CallFromIndex,
                            *address,
                            from_block..=to_block,
                        )?;

                        if !bitmap.is_empty() {
                            blocks_to_scan.or_inplace(&bitmap);
                            from_addresses.insert(*address);
                        }
                    }

                    let mut to_blocks = croaring::Treemap::default();

                    for address in &requested_to_addresses {
                        let bitmap = bitmapdb::get(
                            &txn,
                            tables::CallToIndex,
                            *address,
                            from_block..=to_block,
                        )?;

                        if !bitmap.is_empty() {
                            to_blocks.or_inplace(&bitmap);
                            to_addresses.insert(*address);
                        }
                    }

                    (match mode {
                        TraceFilterMode::Union => croaring::Treemap::or_inplace,
                        TraceFilterMode::Intersection => croaring::Treemap::and_inplace,
                    })(&mut blocks_to_scan, &to_blocks);

                    let blocks_to_scan: BTreeSet<_> = if requested_from_addresses.is_empty()
                        && requested_to_addresses.is_empty()
                    {
                        (from_block..=to_block).collect()
                    } else {
                        blocks_to_scan
                            .iter()
                            .filter_map(|v| {
                                if v >= *from_block && v <= *to_block {
                                    Some(BlockNumber(v))
                                } else {
                                    None
                                }
                            })
                            .collect()
                    };

                    for block in blocks_to_scan {
                        trace!("Tracing block {block}");

                        let (_, _, traces) =
                            replay_block(&txn, types::BlockNumber::Number(block.0.into()).into())?
                                .unwrap();

                        for trace in traces.into_iter().filter(|trace| {
                            if requested_from_addresses.is_empty()
                                && requested_to_addresses.is_empty()
                            {
                                return true;
                            }

                            match trace.trace.action {
                                types::Action::Call(types::CallAction { from, to, .. }) => {
                                    return requested_from_addresses.contains(&from)
                                        || requested_to_addresses.contains(&to);
                                }
                                types::Action::Create(types::CreateAction { from, .. }) => {
                                    if requested_from_addresses.contains(&from) {
                                        return true;
                                    }

                                    if let Some(types::TraceResult::Success {
                                        result:
                                            types::TraceOutput::Create(types::CreateOutput {
                                                address,
                                                ..
                                            }),
                                    }) = &trace.trace.result
                                    {
                                        if requested_to_addresses.contains(address) {
                                            return true;
                                        }
                                    }
                                }
                                types::Action::Selfdestruct(types::SelfdestructAction {
                                    address,
                                    refund_address,
                                    ..
                                }) => {
                                    return requested_from_addresses.contains(&address)
                                        || requested_to_addresses.contains(&refund_address);
                                }
                                types::Action::Reward(types::RewardAction { author, .. }) => {
                                    return requested_to_addresses.contains(&author);
                                }
                            }

                            false
                        }) {
                            if res_tx.blocking_send(Ok(trace)).is_err() {
                                return Ok(());
                            };
                        }
                    }

                    Ok(())
                }
            };
            if let Err::<_, anyhow::Error>(e) = (f)() {
                let _ = res_tx.blocking_send(Err(e));
            }
        });

        tokio_stream::wrappers::ReceiverStream::new(rx)
    }
}

#[async_trait]
impl<DB> TraceApiServer for TraceApiServerImpl<DB>
where
    DB: EnvironmentKind,
{
    async fn call(
        &self,
        call: types::MessageCall,
        trace_types: HashSet<types::TraceType>,
        block_number: Option<types::BlockId>,
    ) -> RpcResult<types::FullTrace> {
        Ok(self
            .call_many(vec![(call, trace_types)], block_number)
            .await?
            .remove(0))
    }

    async fn call_many(
        &self,
        calls: Vec<(types::MessageCall, HashSet<types::TraceType>)>,
        block_id: Option<types::BlockId>,
    ) -> RpcResult<Vec<types::FullTrace>> {
        let db = self.db.clone();
        let call_gas_limit = self.call_gas_limit;

        tokio::task::spawn_blocking(move || {
            let txn = db.begin()?;

            let block_id = block_id.unwrap_or(types::BlockId::Number(types::BlockNumber::Latest));

            let (block_number, block_hash) = helpers::resolve_block_id(&txn, block_id)?
                .ok_or_else(|| format_err!("failed to resolve block {block_id:?}"))?;
            let historical = matches!(block_id, types::BlockId::Number(types::BlockNumber::Latest));

            let chain_id = txn
                .get(tables::Config, ())?
                .ok_or_else(|| format_err!("chain spec not found"))?
                .params
                .chain_id;

            let header = crate::accessors::chain::header::read(&txn, block_hash, block_number)?
                .ok_or_else(|| format_err!("header not found"))?;

            let msgs = calls
                .into_iter()
                .map(|(call, trace_types)| {
                    let (sender, message) = helpers::convert_message_call(
                        &Buffer::new(&txn, if historical { Some(block_number) } else { None }),
                        chain_id,
                        call,
                        &header,
                        U256::ZERO,
                        Some(call_gas_limit),
                    )?;
                    Ok((sender, message, trace_types))
                })
                .collect::<anyhow::Result<_>>()?;

            Ok(do_call_many(&txn, CallManyMode::Speculative(block_id), msgs, None)?.0)
        })
        .await
        .unwrap_or_else(helpers::joinerror_to_result)
    }

    async fn raw_transaction(
        &self,
        rlp: types::Bytes,
        trace_types: HashSet<types::TraceType>,
        block_id: Option<types::BlockId>,
    ) -> RpcResult<types::FullTrace> {
        let db = self.db.clone();

        tokio::task::spawn_blocking(move || {
            let signed_message = <MessageWithSignature as fastrlp::Decodable>::decode(&mut &*rlp.0)
                .map_err(|e| format_err!("failed to decode tx: {e}"))?;

            let sender = signed_message.recover_sender()?;

            let txn = db.begin()?;

            let block_id = block_id.unwrap_or(types::BlockId::Number(types::BlockNumber::Latest));

            Ok(do_call_many(
                &txn,
                CallManyMode::Speculative(block_id),
                vec![(sender, signed_message.message, trace_types)],
                None,
            )?
            .0
            .remove(0))
        })
        .await
        .unwrap_or_else(helpers::joinerror_to_result)
    }

    async fn replay_block_transactions(
        &self,
        block_id: types::BlockId,
        trace_types: HashSet<types::TraceType>,
    ) -> RpcResult<Option<Vec<types::FullTraceWithTransactionHash>>> {
        let db = self.db.clone();

        tokio::task::spawn_blocking(move || {
            let txn = db.begin()?;

            Ok(
                replay_block_transactions(&txn, block_id, trace_types, false)?
                    .map(|(_, _, full_traces, _)| full_traces),
            )
        })
        .await
        .unwrap_or_else(helpers::joinerror_to_result)
    }

    async fn replay_transaction(
        &self,
        hash: H256,
        trace_types: HashSet<types::TraceType>,
    ) -> RpcResult<types::FullTrace> {
        let db = self.db.clone();

        tokio::task::spawn_blocking(move || {
            let txn = db.begin()?;
            if let Some(block_number) = crate::accessors::chain::tl::read(&txn, hash)? {
                let block_hash = crate::accessors::chain::canonical_hash::read(&txn, block_number)?
                    .ok_or_else(|| format_err!("canonical hash for block #{block_number} not found"))?;
                let transactions = crate::accessors::chain::block_body::read_without_senders(
                        &txn,
                        block_hash,
                        block_number,
                    )?.ok_or_else(|| format_err!("body not found for block #{block_number}/{block_hash}"))?
                    .transactions;
                let (index, signed_message) = transactions
                    .iter()
                    .enumerate()
                    .find(|(_, tx)| tx.hash() == hash)
                    .ok_or_else(|| {
                        format_err!(
                            "tx with hash {hash} is not found in block #{block_number}/{block_hash} - tx lookup index invalid?"
                        )
                    })?;

                let message = signed_message.message.clone();
                let senders = crate::accessors::chain::tx_sender::read(&txn, block_hash, block_number)?;
                let sender = *senders
                    .get(index)
                    .ok_or_else(|| format_err!("senders too short: {index} vs len {}", senders.len()))?;

                return Ok(do_call_many(
                    &txn,
                    CallManyMode::Replay(types::BlockNumber::Number(block_number.0.into()).into()),
                    transactions
                        .into_iter()
                        .zip(senders)
                        .map(|(signed_message, sender)| (sender, signed_message.message, hashset![]))
                        .take(index)
                        .chain(once((sender, message, trace_types)))
                        .collect(),None,
                )?.0
                .pop().unwrap());
            }

            Err(RpcError::Custom("tx not found".to_string()))
        })
        .await
        .unwrap_or_else(helpers::joinerror_to_result)
    }

    async fn block(
        &self,
        block_id: types::BlockId,
    ) -> RpcResult<Option<Vec<types::TransactionTraceWithLocation>>> {
        let db = self.db.clone();

        tokio::task::spawn_blocking(move || {
            let txn = db.begin()?;

            Ok(
                replay_block_transactions(&txn, block_id, hashset![types::TraceType::Trace], true)?
                    .map(|(block_number, block_hash, full_traces, rewards)| {
                        full_traces
                            .into_iter()
                            .enumerate()
                            .flat_map(
                                |(
                                    transaction_position,
                                    types::FullTraceWithTransactionHash {
                                        full_trace,
                                        transaction_hash,
                                    },
                                )| {
                                    full_trace.trace.unwrap_or_default().into_iter().map(
                                        move |trace| TransactionTraceWithLocation {
                                            trace,
                                            transaction_position: Some(transaction_position),
                                            transaction_hash: Some(transaction_hash),
                                            block_number: block_number.0.into(),
                                            block_hash,
                                        },
                                    )
                                },
                            )
                            .chain(
                                rewards
                                    .into_iter()
                                    .map(|reward| TransactionTraceWithLocation {
                                        trace: types::TransactionTrace {
                                            trace_address: vec![],
                                            subtraces: 0,
                                            action: types::Action::Reward(reward),
                                            result: None,
                                        },
                                        transaction_position: None,
                                        transaction_hash: None,
                                        block_number: block_number.0.into(),
                                        block_hash,
                                    }),
                            )
                            .collect()
                    }),
            )
        })
        .await
        .unwrap_or_else(helpers::joinerror_to_result)
    }

    async fn filter(
        &self,
        filter: ethereum_jsonrpc::Filter,
    ) -> RpcResult<Vec<types::TransactionTraceWithLocation>> {
        let skip = filter.after.unwrap_or(0);
        let take = filter.count.unwrap_or(usize::MAX);
        let mut rx = self.filter_stream(filter).skip(skip).take(take);

        let mut traces = Vec::new();
        while let Some(trace) = rx.try_next().await? {
            traces.push(trace);
        }
        Ok(traces)
    }
}

mod convert {
    use super::*;
    use ethereum_interfaces::web3::{Eip1559Call, Eip2930Call, LegacyCall, TraceKinds};

    pub fn access_list(access_list: web3::AccessList) -> Option<Vec<types::AccessListEntry>> {
        access_list
            .access_list
            .into_iter()
            .map(|web3::AccessListItem { address, slots }| {
                address.map(|address| types::AccessListEntry {
                    address: address.into(),
                    storage_keys: slots.into_iter().map(|slot| slot.into()).collect(),
                })
            })
            .collect::<Option<Vec<_>>>()
    }

    pub fn message_call(call: web3::Call) -> Option<types::MessageCall> {
        Some(match call.call? {
            web3::call::Call::Legacy(LegacyCall {
                from,
                to,
                gas_limit,
                gas_price,
                value,
                input,
            }) => types::MessageCall::Legacy {
                from: from.map(From::from),
                to: to.map(From::from),
                gas: gas_limit.map(From::from),
                gas_price: gas_price.map(From::from),
                value: value.map(From::from),
                data: input.map(From::from),
                tag: Default::default(),
            },
            web3::call::Call::Eip2930(Eip2930Call {
                from,
                to,
                gas_limit,
                gas_price,
                value,
                input,
                access_list,
            }) => types::MessageCall::EIP2930 {
                from: from.map(From::from),
                to: to.map(From::from),
                gas: gas_limit.map(From::from),
                gas_price: gas_price.map(From::from),
                value: value.map(From::from),
                data: input.map(From::from),
                access_list: if let Some(access_list) = access_list {
                    Some(self::access_list(access_list)?)
                } else {
                    None
                },
                tag: Default::default(),
            },
            web3::call::Call::Eip1559(Eip1559Call {
                from,
                to,
                gas_limit,
                max_priority_fee_per_gas,
                max_fee_per_gas,
                value,
                input,
                access_list,
            }) => types::MessageCall::EIP1559 {
                from: from.map(From::from),
                to: to.map(From::from),
                gas: gas_limit.map(From::from),
                max_priority_fee_per_gas: max_priority_fee_per_gas.map(From::from),
                max_fee_per_gas: max_fee_per_gas.map(From::from),
                value: value.map(From::from),
                data: input.map(From::from),
                access_list: if let Some(access_list) = access_list {
                    Some(self::access_list(access_list)?)
                } else {
                    None
                },
                tag: Default::default(),
            },
        })
    }

    pub fn trace_kinds(trace_kinds: TraceKinds) -> HashSet<types::TraceType> {
        let mut kinds = HashSet::with_capacity(3);
        if trace_kinds.trace {
            kinds.insert(types::TraceType::Trace);
        }
        if trace_kinds.vm_trace {
            kinds.insert(types::TraceType::VmTrace);
        }
        if trace_kinds.state_diff {
            kinds.insert(types::TraceType::StateDiff);
        }

        kinds
    }

    pub fn vm_trace(types::VmTrace { code, ops }: types::VmTrace) -> web3::VmTrace {
        web3::VmTrace {
            code: code.into(),
            ops: ops
                .into_iter()
                .map(
                    |types::VmInstruction { pc, cost, ex, sub }| web3::VmInstruction {
                        pc: pc as u32,
                        cost: cost as u64,
                        ex: ex.map(
                            |types::VmExecutedOperation {
                                 used,
                                 push,
                                 mem,
                                 store,
                             }| {
                                web3::VmExecutedOperation {
                                    used,
                                    push: push.map(From::from),
                                    mem: mem.map(|types::MemoryDelta { off, data }| {
                                        web3::MemoryDelta {
                                            off: off as u64,
                                            data: data.into(),
                                        }
                                    }),
                                    store: store.map(|types::StorageDelta { key, val }| {
                                        web3::StorageDelta {
                                            key: Some(key.into()),
                                            val: Some(val.into()),
                                        }
                                    }),
                                }
                            },
                        ),
                        sub: sub.map(self::vm_trace),
                    },
                )
                .collect(),
        }
    }

    pub trait DeltaConvert: Sized {
        type Out;

        fn convert(v: types::Delta<Self>) -> Self::Out;
    }

    impl DeltaConvert for U64 {
        type Out = web3::DeltaU64;

        fn convert(v: types::Delta<Self>) -> Self::Out {
            Self::Out {
                delta: Some(match v {
                    types::Delta::Unchanged => web3::delta_u64::Delta::Unchanged(()),
                    types::Delta::Added(v) => web3::delta_u64::Delta::Added(v.as_u64()),
                    types::Delta::Removed(v) => web3::delta_u64::Delta::Removed(v.as_u64()),
                    types::Delta::Altered(types::AlteredType { from, to }) => {
                        web3::delta_u64::Delta::Altered(web3::AlteredU64 {
                            from: from.as_u64(),
                            to: to.as_u64(),
                        })
                    }
                }),
            }
        }
    }

    impl DeltaConvert for U256 {
        type Out = web3::DeltaH256;

        fn convert(v: types::Delta<Self>) -> Self::Out {
            Self::Out {
                delta: Some(match v {
                    types::Delta::Unchanged => web3::delta_h256::Delta::Unchanged(()),
                    types::Delta::Added(v) => web3::delta_h256::Delta::Added(v.into()),
                    types::Delta::Removed(v) => web3::delta_h256::Delta::Removed(v.into()),
                    types::Delta::Altered(types::AlteredType { from, to }) => {
                        web3::delta_h256::Delta::Altered(web3::AlteredH256 {
                            from: Some(from.into()),
                            to: Some(to.into()),
                        })
                    }
                }),
            }
        }
    }

    impl DeltaConvert for H256 {
        type Out = web3::DeltaH256;

        fn convert(v: types::Delta<Self>) -> Self::Out {
            Self::Out {
                delta: Some(match v {
                    types::Delta::Unchanged => web3::delta_h256::Delta::Unchanged(()),
                    types::Delta::Added(v) => web3::delta_h256::Delta::Added(v.into()),
                    types::Delta::Removed(v) => web3::delta_h256::Delta::Removed(v.into()),
                    types::Delta::Altered(types::AlteredType { from, to }) => {
                        web3::delta_h256::Delta::Altered(web3::AlteredH256 {
                            from: Some(from.into()),
                            to: Some(to.into()),
                        })
                    }
                }),
            }
        }
    }

    impl DeltaConvert for types::Bytes {
        type Out = web3::DeltaBytes;

        fn convert(v: types::Delta<Self>) -> Self::Out {
            Self::Out {
                delta: Some(match v {
                    types::Delta::Unchanged => web3::delta_bytes::Delta::Unchanged(()),
                    types::Delta::Added(v) => web3::delta_bytes::Delta::Added(v.into()),
                    types::Delta::Removed(v) => web3::delta_bytes::Delta::Removed(v.into()),
                    types::Delta::Altered(types::AlteredType { from, to }) => {
                        web3::delta_bytes::Delta::Altered(web3::AlteredBytes {
                            from: from.into(),
                            to: to.into(),
                        })
                    }
                }),
            }
        }
    }

    pub fn trace(
        types::TransactionTrace {
            trace_address,
            subtraces,
            action,
            result,
        }: types::TransactionTrace,
    ) -> web3::Trace {
        web3::Trace {
            action: Some(web3::Action {
                action: Some(match action {
                    types::Action::Call(types::CallAction {
                        from,
                        to,
                        value,
                        gas,
                        input,
                        call_type,
                    }) => web3::action::Action::Call(web3::CallAction {
                        from: Some(from.into()),
                        to: Some(to.into()),
                        value: Some(value.into()),
                        gas: gas.as_u64(),
                        input: input.into(),
                        call_type: match call_type {
                            types::CallType::None => None,
                            types::CallType::Call => Some(web3::CallType::Call as i32),
                            types::CallType::CallCode => Some(web3::CallType::CallCode as i32),
                            types::CallType::DelegateCall => {
                                Some(web3::CallType::DelegateCall as i32)
                            }
                            types::CallType::StaticCall => Some(web3::CallType::StaticCall as i32),
                        },
                    }),
                    types::Action::Create(types::CreateAction {
                        from,
                        value,
                        gas,
                        init,
                    }) => web3::action::Action::Create(web3::CreateAction {
                        from: Some(from.into()),
                        value: Some(value.into()),
                        gas: gas.as_u64(),
                        init: init.into(),
                    }),
                    types::Action::Selfdestruct(types::SelfdestructAction {
                        address,
                        refund_address,
                        balance,
                    }) => web3::action::Action::Selfdestruct(web3::SelfdestructAction {
                        address: Some(address.into()),
                        refund_address: Some(refund_address.into()),
                        balance: Some(balance.into()),
                    }),
                    types::Action::Reward(types::RewardAction {
                        author,
                        value,
                        reward_type,
                    }) => web3::action::Action::Reward(web3::RewardAction {
                        author: Some(author.into()),
                        value: Some(value.into()),
                        reward_type: match reward_type {
                            types::RewardType::Block => {
                                web3::reward_action::RewardType::Block as i32
                            }
                            types::RewardType::Uncle => {
                                web3::reward_action::RewardType::Uncle as i32
                            }
                        },
                    }),
                }),
            }),
            result: Some(web3::TraceResult {
                result: result.map(|result| match result {
                    types::TraceResult::Success { result } => {
                        web3::trace_result::Result::Output(web3::TraceOutput {
                            output: Some(match result {
                                types::TraceOutput::Call(types::CallOutput {
                                    gas_used,
                                    output,
                                }) => web3::trace_output::Output::Call(web3::CallOutput {
                                    gas_used: gas_used.as_u64(),
                                    output: output.into(),
                                }),
                                types::TraceOutput::Create(types::CreateOutput {
                                    gas_used,
                                    code,
                                    address,
                                }) => web3::trace_output::Output::Create(web3::CreateOutput {
                                    gas_used: gas_used.as_u64(),
                                    code: code.into(),
                                    address: Some(address.into()),
                                }),
                            }),
                        })
                    }
                    types::TraceResult::Error { error } => web3::trace_result::Result::Error(error),
                }),
            }),
            subtraces: subtraces as u64,
            trace_address: trace_address.into_iter().map(|v| v as u64).collect(),
        }
    }

    pub fn trace_with_location(
        types::TransactionTraceWithLocation {
            trace,
            transaction_position,
            transaction_hash,
            block_number,
            block_hash,
        }: types::TransactionTraceWithLocation,
    ) -> web3::TraceWithLocation {
        web3::TraceWithLocation {
            trace: Some(self::trace(trace)),
            transaction_position: transaction_position.map(|v| v as u64),
            transaction_hash: transaction_hash.map(|v| v.into()),
            block_number: block_number.as_u64(),
            block_hash: Some(block_hash.into()),
        }
    }

    pub fn full_trace(
        types::FullTrace {
            output,
            trace,
            vm_trace,
            state_diff,
        }: types::FullTrace,
    ) -> web3::FullTrace {
        web3::FullTrace {
            output: output.0,
            traces: trace.map(|traces| web3::Traces {
                traces: traces.into_iter().map(self::trace).collect(),
            }),
            vm_trace: vm_trace.map(self::vm_trace),
            state_diff: state_diff.map(|types::StateDiff(diff)| web3::StateDiff {
                diff: diff
                    .into_iter()
                    .map(
                        |(
                            key,
                            types::AccountDiff {
                                balance,
                                nonce,
                                code,
                                storage,
                            },
                        )| web3::AccountDiffEntry {
                            key: Some(key.into()),
                            value: Some(web3::AccountDiff {
                                balance: Some(DeltaConvert::convert(balance)),
                                nonce: Some(DeltaConvert::convert(nonce)),
                                code: Some(DeltaConvert::convert(code)),
                                storage: storage
                                    .into_iter()
                                    .map(|(location, delta)| web3::StorageDiffEntry {
                                        location: Some(location.into()),
                                        delta: Some(DeltaConvert::convert(delta)),
                                    })
                                    .collect(),
                            }),
                        },
                    )
                    .collect(),
            }),
        }
    }
}

#[async_trait]
impl<DB> TraceApi for TraceApiServerImpl<DB>
where
    DB: EnvironmentKind,
{
    type FilterStream = BoxStream<'static, Result<TraceWithLocation, tonic::Status>>;

    async fn call(
        &self,
        request: tonic::Request<CallRequests>,
    ) -> Result<Response<FullTraces>, tonic::Status> {
        let CallRequests { calls, block_id } = request.into_inner();
        let block_id = block_id
            .map(helpers::grpc_block_id)
            .ok_or_else(|| tonic::Status::invalid_argument("invalid block id sent"))?;

        <Self as TraceApiServer>::call_many(
            self,
            calls
                .into_iter()
                .map(|CallRequest { call, kinds }| {
                    Ok::<_, tonic::Status>((
                        convert::message_call(
                            call.ok_or_else(|| tonic::Status::invalid_argument("invalid call"))?,
                        )
                        .ok_or_else(|| tonic::Status::invalid_argument("invalid call"))?,
                        convert::trace_kinds(
                            kinds
                                .ok_or_else(|| tonic::Status::invalid_argument("invalid kinds"))?,
                        ),
                    ))
                })
                .collect::<Result<Vec<_>, tonic::Status>>()?,
            block_id,
        )
        .await
        .map(|full_traces| {
            Response::new(FullTraces {
                traces: full_traces.into_iter().map(convert::full_trace).collect(),
            })
        })
        .map_err(|e| tonic::Status::internal(e.to_string()))
    }

    async fn block(
        &self,
        request: tonic::Request<ethereum_interfaces::web3::BlockId>,
    ) -> Result<Response<OptionalTracesWithLocation>, tonic::Status> {
        let block_id = helpers::grpc_block_id(request.into_inner())
            .ok_or_else(|| tonic::Status::invalid_argument("invalid block id sent"))?;

        <Self as TraceApiServer>::block(self, block_id)
            .await
            .map(|traces| {
                Response::new(OptionalTracesWithLocation {
                    traces: traces.map(|traces| TracesWithLocation {
                        traces: traces
                            .into_iter()
                            .map(convert::trace_with_location)
                            .collect(),
                    }),
                })
            })
            .map_err(|e| tonic::Status::internal(e.to_string()))
    }

    async fn block_transactions(
        &self,
        request: tonic::Request<TraceBlockRequest>,
    ) -> Result<Response<OptionalFullTracesWithTransactionHashes>, tonic::Status> {
        let TraceBlockRequest { id, kinds } = request.into_inner();

        <Self as TraceApiServer>::replay_block_transactions(
            self,
            id.and_then(helpers::grpc_block_id)
                .ok_or_else(|| tonic::Status::invalid_argument("invalid block id sent"))?,
            kinds
                .map(convert::trace_kinds)
                .ok_or_else(|| tonic::Status::invalid_argument("invalid trace kinds requested"))?,
        )
        .await
        .map(|opt| {
            tonic::Response::new(OptionalFullTracesWithTransactionHashes {
                traces: opt.map(|full_traces| web3::FullTracesWithTransactionHashes {
                    traces: full_traces
                        .into_iter()
                        .map(
                            |ethereum_jsonrpc::types::FullTraceWithTransactionHash {
                                 full_trace,
                                 transaction_hash,
                             }| web3::FullTraceWithTransactionHash {
                                full_trace: Some(convert::full_trace(full_trace)),
                                transaction_hash: Some(transaction_hash.into()),
                            },
                        )
                        .collect(),
                }),
            })
        })
        .map_err(|e| tonic::Status::internal(e.to_string()))
    }

    async fn transaction(
        &self,
        request: tonic::Request<TraceTransactionRequest>,
    ) -> Result<Response<FullTrace>, tonic::Status> {
        let TraceTransactionRequest { hash, kinds } = request.into_inner();

        <Self as TraceApiServer>::replay_transaction(
            self,
            hash.map(From::from)
                .ok_or_else(|| tonic::Status::invalid_argument("invalid tx hash"))?,
            kinds
                .map(convert::trace_kinds)
                .ok_or_else(|| tonic::Status::invalid_argument("invalid trace kinds requested"))?,
        )
        .await
        .map(|trace| tonic::Response::new(convert::full_trace(trace)))
        .map_err(|e| tonic::Status::internal(e.to_string()))
    }

    async fn filter(
        &self,
        request: tonic::Request<FilterRequest>,
    ) -> Result<tonic::Response<Self::FilterStream>, tonic::Status> {
        let FilterRequest {
            from_block,
            to_block,
            from_addresses,
            to_addresses,
            mode,
        } = request.into_inner();

        let filter = ethereum_jsonrpc::Filter {
            from_block: from_block.and_then(helpers::grpc_block_id),
            to_block: to_block.and_then(helpers::grpc_block_id),
            from_address: from_addresses
                .map(|addresses| addresses.addresses.into_iter().map(Address::from).collect()),
            to_address: to_addresses
                .map(|addresses| addresses.addresses.into_iter().map(Address::from).collect()),
            mode: mode.and_then(|mode| {
                web3::FilterMode::from_i32(mode).map(|mode| match mode {
                    web3::FilterMode::Union => TraceFilterMode::Union,
                    web3::FilterMode::Intersection => TraceFilterMode::Intersection,
                })
            }),
            after: None,
            count: None,
        };

        Ok(tonic::Response::new(Box::pin(
            self.filter_stream(filter).map(|res| {
                res.map(|transaction_trace_with_location| {
                    convert::trace_with_location(transaction_trace_with_location)
                })
                .map_err(|e| tonic::Status::internal(e.to_string()))
            }),
        )))
    }
}
