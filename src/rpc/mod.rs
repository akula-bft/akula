pub mod eth;

mod helpers {
    use crate::{
        accessors::chain,
        kv::tables,
        models::{BlockNumber, Message, TransactionAction},
        stagedsync::stages,
    };
    use ethereum_jsonrpc::types;
    use ethereum_types::U64;

    pub async fn get_block_number<'db, Tx: crate::kv::traits::Transaction<'db>>(
        txn: &Tx,
        block_number: ethereum_jsonrpc::types::BlockNumber,
    ) -> anyhow::Result<BlockNumber> {
        let block_number = match block_number {
            types::BlockNumber::Number(n) => BlockNumber(n.as_u64()),
            types::BlockNumber::Earliest => BlockNumber(0),
            _ => txn
                .get(tables::SyncStage, stages::FINISH)
                .await?
                .unwrap_or(BlockNumber(0)),
        };
        Ok(block_number)
    }

    pub async fn construct_block<'db, Tx: crate::kv::traits::Transaction<'db>>(
        txn: &Tx,
        block_id: types::BlockId,
        include_txs: Option<bool>,
        uncle_index: Option<U64>,
    ) -> anyhow::Result<types::Block> {
        let (block_number, block_hash, header) = match block_id {
            types::BlockId::Number(n) => {
                let block_number = get_block_number(txn, n).await?;
                let block_hash = txn
                    .get(tables::CanonicalHeader, block_number)
                    .await?
                    .unwrap();
                match uncle_index {
                    Some(n) => {
                        let body = txn
                            .get(tables::BlockBody, (block_number, block_hash))
                            .await?
                            .unwrap();
                        let uncle_header = body.uncles.into_iter().nth(n.as_usize()).unwrap();
                        (block_number, uncle_header.hash(), uncle_header)
                    }
                    None => {
                        let header = txn
                            .get(tables::Header, (block_number, block_hash))
                            .await?
                            .unwrap();
                        (block_number, block_hash, header)
                    }
                }
            }
            types::BlockId::Hash(block_hash) => {
                let block_number = txn.get(tables::HeaderNumber, block_hash).await?.unwrap();
                match uncle_index {
                    Some(n) => {
                        let body = txn
                            .get(tables::BlockBody, (block_number, block_hash))
                            .await?
                            .unwrap();
                        let uncle_header = body.uncles.into_iter().nth(n.as_usize()).unwrap();
                        (block_number, uncle_header.hash(), uncle_header)
                    }
                    None => {
                        let header = txn
                            .get(tables::Header, (block_number, block_hash))
                            .await?
                            .unwrap();
                        (block_number, block_hash, header)
                    }
                }
            }
        };

        let body = chain::block_body::read_without_senders(txn, block_hash, block_number)
            .await?
            .unwrap();

        let transactions: Vec<types::Tx> = match include_txs.unwrap_or(true) {
            true => {
                let senders = chain::tx_sender::read(txn, block_hash, block_number).await?;
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
                            to: match tx.message.action() {
                                TransactionAction::Call(to) => Some(to),
                                TransactionAction::Create => None,
                            },
                            transaction_index: Some(U64::from(index as u64)),
                            value: tx.message.value(),
                            v: U64::from(tx.v()),
                            r: tx.r(),
                            s: tx.s(),
                        }))
                    })
                    .collect()
            }
            false => body
                .transactions
                .into_iter()
                .map(|tx| types::Tx::Hash(tx.message.hash()))
                .collect(),
        };

        let td = chain::td::read(txn, block_hash, block_number).await?;

        Ok(types::Block {
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
        })
    }
}
