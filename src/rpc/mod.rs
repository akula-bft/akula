pub mod eth;

mod helpers {
    use crate::{
        accessors::chain,
        kv::{mdbx::*, tables},
        models::*,
        stagedsync::stages,
    };
    use anyhow::format_err;
    use ethereum_jsonrpc::types;
    use ethereum_types::U64;

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
                                    hash: tx.message.hash(),
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
                            .map(|tx| types::Tx::Hash(tx.message.hash()))
                            .collect()
                    };

                    let td = chain::td::read(txn, block_hash, block_number)?;

                    return Ok(Some(types::Block {
                        number: Some(U64::from(block_number.0)),
                        hash: Some(block_hash),
                        parent_hash: header.parent_hash,
                        nonce: Some(header.nonce),
                        sha3_uncles: header.ommers_hash,
                        logs_bloom: Some(header.logs_bloom),
                        transactions_root: header.transactions_root,
                        state_root: header.state_root,
                        receipts_root: header.receipts_root,
                        miner: header.beneficiary,
                        difficulty: header.difficulty,
                        total_difficulty: td,
                        seal_fields: (header.mix_hash, header.nonce),
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
}
