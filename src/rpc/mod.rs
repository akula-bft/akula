pub mod erigon;
pub mod eth;
pub mod net;
pub mod otterscan;
pub mod trace;
pub mod helpers {
    use crate::{
        accessors::chain,
        consensus::{engine_factory, DuoError},
        execution::{
            analysis_cache::AnalysisCache, processor::ExecutionProcessor, tracer::NoopTracer,
        },
        kv::{mdbx::*, tables},
        models::*,
        stagedsync::stages,
        Buffer, StateReader,
    };
    use anyhow::format_err;
    use ethereum_jsonrpc::types;
    use ethereum_types::U64;
    use jsonrpsee::core::Error as RpcError;

    impl From<DuoError> for RpcError {
        fn from(e: DuoError) -> Self {
            match e {
                DuoError::Validation(e) => format_err!("validation error: {:?}", e).into(),
                DuoError::Internal(e) => e.into(),
            }
        }
    }

    impl From<types::AccessListEntry> for AccessListItem {
        fn from(
            types::AccessListEntry {
                address,
                storage_keys,
            }: types::AccessListEntry,
        ) -> Self {
            Self {
                address,
                slots: storage_keys.into_iter().collect(),
            }
        }
    }

    pub fn resolve_block_number<K: TransactionKind, E: EnvironmentKind>(
        txn: &MdbxTransaction<'_, K, E>,
        block_number: ethereum_jsonrpc::types::BlockNumber,
    ) -> anyhow::Result<BlockNumber> {
        match block_number {
            types::BlockNumber::Latest | types::BlockNumber::Pending => txn
                .get(tables::SyncStage, stages::FINISH)
                .and_then(|b| b.ok_or_else(|| format_err!("sync progress not found"))),
            types::BlockNumber::Earliest => txn
                .get(tables::PruneProgress, stages::FINISH)
                .and_then(|b| b.ok_or_else(|| format_err!("prune progress not found"))),
            types::BlockNumber::Number(number) => Ok(number.as_u64().into()),
        }
    }

    pub fn resolve_block_id<K: TransactionKind, E: EnvironmentKind>(
        txn: &MdbxTransaction<'_, K, E>,
        block_id: impl Into<ethereum_jsonrpc::types::BlockId>,
    ) -> anyhow::Result<Option<(BlockNumber, H256)>> {
        match block_id.into() {
            types::BlockId::Hash(hash) => Ok(txn
                .get(tables::HeaderNumber, hash)?
                .map(|number| (number, hash))),
            types::BlockId::Number(number) => {
                let number = resolve_block_number(txn, number)?;
                Ok(txn
                    .get(tables::CanonicalHeader, number)?
                    .map(|hash| (number, hash)))
            }
        }
    }

    pub fn construct_block<K: TransactionKind, E: EnvironmentKind>(
        txn: &MdbxTransaction<'_, K, E>,
        block_id: impl Into<types::BlockId>,
        include_txs: bool,
        uncle_index: Option<U64>,
    ) -> anyhow::Result<Option<types::Block>> {
        if let Some((block_number, block_hash)) = resolve_block_id(txn, block_id)? {
            if let Some((block_number, block_hash, header)) = {
                if let Some(n) = uncle_index {
                    txn.get(tables::BlockBody, (block_number, block_hash))?
                        .and_then(|body| {
                            body.uncles
                                .get(n.as_usize())
                                .cloned()
                                .map(|uncle| (uncle.number, uncle.hash(), uncle))
                        })
                } else {
                    txn.get(tables::Header, (block_number, block_hash))?
                        .map(|header| (block_number, block_hash, header))
                }
            } {
                if let Some(body) =
                    chain::block_body::read_without_senders(txn, block_hash, block_number)?
                {
                    let transactions: Vec<types::Tx> = if include_txs {
                        let senders = chain::tx_sender::read(txn, block_hash, block_number)?;
                        body.transactions
                            .into_iter()
                            .zip(senders)
                            .enumerate()
                            .map(|(index, (tx, sender))| {
                                types::Tx::Transaction(Box::new(types::Transaction {
                                    block_number: Some(U64::from(block_number.0)),
                                    block_hash: Some(block_hash),
                                    from: sender,
                                    gas: U64::from(tx.message.gas_limit()),
                                    gas_price: match tx.message {
                                        Message::Legacy { gas_price, .. } => gas_price,
                                        Message::EIP2930 { gas_price, .. } => gas_price,
                                        Message::EIP1559 {
                                            max_fee_per_gas, ..
                                        } => max_fee_per_gas,
                                    },
                                    hash: tx.hash(),
                                    input: tx.message.input().clone().into(),
                                    nonce: U64::from(tx.message.nonce()),
                                    to: tx.message.action().into_address(),
                                    transaction_index: Some(U64::from(index as u64)),
                                    value: tx.message.value(),
                                    v: U64::from(tx.v()),
                                    r: tx.r(),
                                    s: tx.s(),
                                }))
                            })
                            .collect()
                    } else {
                        body.transactions
                            .into_iter()
                            .map(|tx| types::Tx::Hash(tx.hash()))
                            .collect()
                    };

                    let td = chain::td::read(txn, block_hash, block_number)?;

                    return Ok(Some(types::Block {
                        number: Some(U64::from(block_number.0)),
                        hash: Some(block_hash),
                        parent_hash: header.parent_hash,
                        sha3_uncles: header.ommers_hash,
                        logs_bloom: Some(header.logs_bloom),
                        transactions_root: header.transactions_root,
                        state_root: header.state_root,
                        receipts_root: header.receipts_root,
                        miner: header.beneficiary,
                        difficulty: header.difficulty,
                        total_difficulty: td,
                        seal_fields: None,
                        nonce: Some(header.nonce),
                        mix_hash: Some(header.mix_hash),
                        extra_data: header.extra_data.into(),
                        size: U64::zero(),
                        gas_limit: U64::from(header.gas_limit),
                        gas_used: U64::from(header.gas_used),
                        timestamp: U64::from(header.timestamp),
                        transactions,
                        uncles: body.ommers.into_iter().map(|uncle| uncle.hash()).collect(),
                    }));
                }
            }
        }

        Ok(None)
    }

    pub fn get_receipts<K: TransactionKind, E: EnvironmentKind>(
        txn: &MdbxTransaction<'_, K, E>,
        block_number: BlockNumber,
    ) -> Result<Vec<types::TransactionReceipt>, DuoError> {
        let block_hash = chain::canonical_hash::read(txn, block_number)?
            .ok_or_else(|| format_err!("no canonical header for block #{block_number:?}"))?;
        let header = chain::header::read(txn, block_hash, block_number)?.ok_or_else(|| {
            format_err!("header not found for block #{block_number}/{block_hash}")
        })?;
        let block_body = chain::block_body::read_with_senders(txn, block_hash, block_number)?
            .ok_or_else(|| format_err!("body not found for block #{block_number}/{block_hash}"))?;
        let chain_spec = chain::chain_config::read(txn)?
            .ok_or_else(|| format_err!("chain specification not found"))?;

        // Prepare the execution context.
        let mut buffer = Buffer::new(txn, Some(BlockNumber(block_number.0 - 1)));

        let block_execution_spec = chain_spec.collect_block_spec(block_number);
        let mut engine = engine_factory(None, chain_spec)?;
        let mut analysis_cache = AnalysisCache::default();
        let mut tracer = NoopTracer;

        let mut processor = ExecutionProcessor::new(
            &mut buffer,
            &mut tracer,
            &mut analysis_cache,
            &mut *engine,
            &header,
            &block_body,
            &block_execution_spec,
        );

        processor
            .execute_block_no_post_validation()
            .map(|receipts| {
                let mut last_cumul_gas_used = 0;
                receipts
                    .into_iter()
                    .enumerate()
                    .map(
                        |(
                            transaction_index,
                            Receipt {
                                success,
                                cumulative_gas_used,
                                bloom,
                                logs,
                                ..
                            },
                        )| {
                            let transaction = &block_body.transactions[transaction_index];
                            let transaction_hash = transaction.hash();
                            let gas_used = (cumulative_gas_used - last_cumul_gas_used).into();
                            last_cumul_gas_used = cumulative_gas_used;
                            types::TransactionReceipt {
                                transaction_hash,
                                transaction_index: U64::from(transaction_index),
                                block_hash,
                                block_number: U64::from(block_number.0),
                                from: transaction.sender,
                                to: transaction.message.action().into_address(),
                                cumulative_gas_used: cumulative_gas_used.into(),
                                gas_used,
                                contract_address: if let TransactionAction::Create =
                                    transaction.message.action()
                                {
                                    Some(crate::execution::address::create_address(
                                        transaction.sender,
                                        transaction.message.nonce(),
                                    ))
                                } else {
                                    None
                                },
                                logs: logs
                                    .into_iter()
                                    .enumerate()
                                    .map(
                                        |(
                                            log_index,
                                            Log {
                                                address,
                                                data,
                                                topics,
                                            },
                                        )| {
                                            types::TransactionLog {
                                                log_index: Some(U64::from(log_index)),
                                                transaction_index: Some(U64::from(
                                                    transaction_index,
                                                )),
                                                transaction_hash: Some(transaction_hash),
                                                block_hash: Some(block_hash),
                                                block_number: Some(U64::from(block_number.0)),
                                                address,
                                                data: data.into(),
                                                topics,
                                            }
                                        },
                                    )
                                    .collect::<Vec<_>>(),
                                logs_bloom: bloom,
                                status: if success {
                                    U64::from(1_u16)
                                } else {
                                    U64::zero()
                                },
                            }
                        },
                    )
                    .collect()
            })
    }

    pub fn convert_message_call<S: StateReader>(
        state: &S,
        chain_id: ChainId,
        call: types::MessageCall,
        header: &BlockHeader,
        default_gas_price: U256,
        default_gas_limit: Option<u64>,
    ) -> anyhow::Result<(Address, Message)> {
        Ok(match call {
            types::MessageCall::Legacy {
                from,
                to,
                gas,
                gas_price,
                value,
                data,
            } => {
                let sender = from.unwrap_or_else(Address::zero);

                let gas_limit = if let Some(gas) = gas {
                    gas.as_u64()
                } else if let Some(gas) = default_gas_limit {
                    gas
                } else {
                    header.gas_limit
                };

                let message = Message::Legacy {
                    chain_id: None,
                    nonce: state
                        .read_account(sender)?
                        .map(|acc| acc.nonce)
                        .unwrap_or_default(),
                    gas_price: gas_price.unwrap_or(default_gas_price),
                    gas_limit,
                    action: if let Some(to) = to {
                        TransactionAction::Call(to)
                    } else {
                        TransactionAction::Create
                    },
                    value: value.unwrap_or_default(),
                    input: data.unwrap_or_default().into(),
                };

                (sender, message)
            }
            types::MessageCall::EIP2930 {
                from,
                to,
                gas,
                gas_price,
                value,
                data,
                access_list,
            } => {
                let sender = from.unwrap_or_else(Address::zero);

                let gas_limit = if let Some(gas) = gas {
                    gas.as_u64()
                } else if let Some(gas) = default_gas_limit {
                    gas
                } else {
                    header.gas_limit
                };

                let message = Message::EIP2930 {
                    chain_id,
                    nonce: state
                        .read_account(sender)?
                        .map(|acc| acc.nonce)
                        .unwrap_or_default(),
                    gas_price: gas_price.unwrap_or(default_gas_price),
                    gas_limit,
                    action: if let Some(to) = to {
                        TransactionAction::Call(to)
                    } else {
                        TransactionAction::Create
                    },
                    value: value.unwrap_or_default(),
                    input: data.unwrap_or_default().into(),
                    access_list: access_list
                        .map(|access_list| access_list.into_iter().map(From::from).collect())
                        .unwrap_or_default(),
                };

                (sender, message)
            }
            types::MessageCall::EIP1559 {
                from,
                to,
                gas,
                max_fee_per_gas,
                max_priority_fee_per_gas,
                value,
                data,
                access_list,
            } => {
                let sender = from.unwrap_or_else(Address::zero);

                let gas_limit = if let Some(gas) = gas {
                    gas.as_u64()
                } else if let Some(gas) = default_gas_limit {
                    gas
                } else {
                    header.gas_limit
                };

                let message = Message::EIP1559 {
                    chain_id,
                    nonce: state
                        .read_account(sender)?
                        .map(|acc| acc.nonce)
                        .unwrap_or_default(),
                    max_fee_per_gas: max_fee_per_gas.unwrap_or(default_gas_price),
                    max_priority_fee_per_gas: max_priority_fee_per_gas.unwrap_or(default_gas_price),
                    gas_limit,
                    action: if let Some(to) = to {
                        TransactionAction::Call(to)
                    } else {
                        TransactionAction::Create
                    },
                    value: value.unwrap_or_default(),
                    input: data.unwrap_or_default().into(),
                    access_list: access_list
                        .map(|access_list| access_list.into_iter().map(From::from).collect())
                        .unwrap_or_default(),
                };

                (sender, message)
            }
        })
    }
}
