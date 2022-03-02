use crate::{
    kv::{mdbx::*, tables},
    models::*,
};
use tracing::*;

pub mod canonical_hash {
    use super::*;

    pub fn read<K: TransactionKind, E: EnvironmentKind>(
        tx: &MdbxTransaction<'_, K, E>,
        number: impl Into<BlockNumber>,
    ) -> anyhow::Result<H256> {
        let number = number.into();
        trace!("Reading canonical hash for block number {}", number);

        Ok(tx.get(tables::CanonicalHeader, number))
    }
}

pub mod header_number {
    use super::*;

    pub fn read<K: TransactionKind, E: EnvironmentKind>(
        tx: &MdbxTransaction<'_, K, E>,
        hash: H256,
    ) -> anyhow::Result<U64> {
        trace!("Reading header number for hash {:?}", hash);

        Ok(tx.get(tables::HeaderNumber, hash))
    }
}

pub mod header {
    use super::*;

    pub fn read<K: TransactionKind, E: EnvironmentKind>(
        tx: &MdbxTransaction<'_, K, E>,
        hash: H256,
        number: impl Into<BlockNumber>,
    ) -> anyhow::Result<H256> {
        let number = number.into();
        trace!("Reading header for block number {}", number);

        Ok(tx.get(tables::Header, (number, hash)))
    }
}

pub mod tx {
    use super::*;

    pub fn read<K: TransactionKind, E: EnvironmentKind>(
        tx: &MdbxTransaction<'_, K, E>,
        base_tx_id: impl Into<TxIndex>,
        amount: usize,
    ) -> anyhow::Result<Vec<MessageWithSignature>> {
        let base_tx_id = base_tx_id.into();
        trace!(
            "Reading {} transactions starting from {}",
            amount,
            base_tx_id
        );

        if amount > 0 {
            tx.cursor(tables::BlockTransaction)?
                .walk(Some(base_tx_id))
                .take(amount)
                .map(|res| res.map(|(_, v)| v))
                .collect()
        } else {
            Ok(vec![])
        }
    }

    pub fn write<'db, E: EnvironmentKind>(
        tx: &MdbxTransaction<'db, RW, E>,
        base_tx_id: impl Into<TxIndex>,
        txs: &[MessageWithSignature],
    ) -> anyhow::Result<()> {
        let base_tx_id = base_tx_id.into();
        trace!(
            "Writing {} transactions starting from {}",
            txs.len(),
            base_tx_id
        );

        let mut cursor = tx.cursor(tables::BlockTransaction).unwrap();

        for (i, eth_tx) in txs.iter().enumerate() {
            cursor.put(base_tx_id + i as u64, eth_tx.clone()).unwrap();
        }

        Ok(())
    }
}

pub mod tx_sender {
    use super::*;

    pub fn read<K: TransactionKind, E: EnvironmentKind>(
        tx: &MdbxTransaction<'_, K, E>,
        hash: H256,
        number: impl Into<BlockNumber>,
    ) -> anyhow::Result<Vec<Address>> {
        let number = number.into();

        trace!(
            "Reading transaction senders for block {}/{:?}",
            number,
            hash
        );

        Ok(tx
            .get(tables::TxSender, (number, hash))?
            .unwrap_or_default())
    }

    pub fn write<E: EnvironmentKind>(
        tx: &MdbxTransaction<'_, RW, E>,
        hash: H256,
        number: impl Into<BlockNumber>,
        senders: Vec<Address>,
    ) -> anyhow::Result<()> {
        let number = number.into();
        trace!(
            "Writing {} transaction senders for block {}/{:?}",
            senders.len(),
            number,
            hash
        );

        tx.set(tables::TxSender, (number, hash), senders).unwrap();

        Ok(())
    }
}

pub mod storage_body {
    use super::*;

    pub fn read<K, E>(
        tx: &MdbxTransaction<'_, K, E>,
        hash: H256,
        number: impl Into<BlockNumber>,
    ) -> anyhow::Result<Option<BodyForStorage>>
    where
        K: TransactionKind,
        E: EnvironmentKind,
    {
        let number = number.into();
        trace!("Reading storage body for block {}/{:?}", number, hash);

        tx.get(tables::BlockBody, (number, hash))
    }

    pub fn has<K, E>(
        tx: &MdbxTransaction<'_, K, E>,
        hash: H256,
        number: impl Into<BlockNumber>,
    ) -> anyhow::Result<bool>
    where
        K: TransactionKind,
        E: EnvironmentKind,
    {
        Ok(read(tx, hash, number)?.is_some())
    }

    pub fn write<E>(
        tx: &MdbxTransaction<'_, RW, E>,
        hash: H256,
        number: impl Into<BlockNumber>,
        body: &BodyForStorage,
    ) -> anyhow::Result<()>
    where
        E: EnvironmentKind,
    {
        let number = number.into();
        trace!("Writing storage body for block {}/{:?}", number, hash);

        tx.set(tables::BlockBody, (number, hash), body.clone())?;

        Ok(())
    }
}

pub mod block_body {
    use super::*;

    fn read_base<K, E>(
        tx: &MdbxTransaction<'_, K, E>,
        hash: H256,
        number: impl Into<BlockNumber>,
    ) -> anyhow::Result<Option<(BlockBody, TxIndex)>>
    where
        K: TransactionKind,
        E: EnvironmentKind,
    {
        if let Some(body) = super::storage_body::read(tx, hash, number)? {
            let transactions = super::tx::read(tx, body.base_tx_id, body.tx_amount.try_into()?)?;

            return Ok(Some((
                BlockBody {
                    transactions,
                    ommers: body.uncles,
                },
                body.base_tx_id,
            )));
        }

        Ok(None)
    }

    pub fn read_without_senders<K: TransactionKind, E: EnvironmentKind>(
        tx: &MdbxTransaction<'_, K, E>,
        hash: H256,
        number: impl Into<BlockNumber>,
    ) -> anyhow::Result<Option<BlockBody>> {
        Ok(read_base(tx, hash, number)?.map(|(v, _)| v))
    }

    pub fn read_with_senders<K: TransactionKind, E: EnvironmentKind>(
        tx: &MdbxTransaction<'_, K, E>,
        hash: H256,
        number: impl Into<BlockNumber>,
    ) -> anyhow::Result<Option<BlockBodyWithSenders>> {
        let number = number.into();
        if let Some((body, _)) = read_base(tx, hash, number)? {
            let senders = super::tx_sender::read(tx, hash, number)?;

            return Ok(Some(BlockBodyWithSenders {
                transactions: body
                    .transactions
                    .into_iter()
                    .zip(senders)
                    .map(|(tx, sender)| MessageWithSender {
                        message: tx.message,
                        sender,
                    })
                    .collect(),
                ommers: body.ommers,
            }));
        }

        Ok(None)
    }
}

pub mod td {
    use super::*;

    pub fn read<K: TransactionKind, E: EnvironmentKind>(
        tx: &MdbxTransaction<'_, K, E>,
        hash: H256,
        number: impl Into<BlockNumber>,
    ) -> anyhow::Result<Option<U256>> {
        let number = number.into();
        trace!("Reading total difficulty at block {}/{:?}", number, hash);

        tx.get(tables::HeadersTotalDifficulty, (number, hash))
    }
}

pub mod tl {
    use super::*;

    pub fn read<K: TransactionKind, E: EnvironmentKind>(
        tx: &MdbxTransaction<'_, K, E>,
        tx_hash: H256,
    ) -> anyhow::Result<Option<BlockNumber>> {
        trace!("Reading Block number for a tx_hash {:?}", tx_hash);

        Ok(tx
            .get(tables::BlockTransactionLookup, tx_hash)?
            .map(|b| b.0))
    }

    pub fn write<'db: 'tx, 'tx, E: EnvironmentKind>(
        tx: &'tx MdbxTransaction<'db, RW, E>,
        hashed_tx_data: H256,
        block_number: BlockNumber,
    ) -> anyhow::Result<()> {
        trace!("Writing tx_lookup for hash {}", hashed_tx_data);

        tx.set(
            tables::BlockTransactionLookup,
            hashed_tx_data,
            block_number.into(),
        )?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kv::new_mem_database;
    use bytes::Bytes;

    #[test]
    fn accessors() {
        let tx1 = MessageWithSignature {
            message: Message::Legacy {
                chain_id: None,
                nonce: 1,
                gas_price: 20_000.as_u256(),
                gas_limit: 3_000_000,
                action: TransactionAction::Create,
                value: 0.as_u256(),
                input: Bytes::new(),
            },
            signature: MessageSignature::new(false, H256::repeat_byte(2), H256::repeat_byte(3))
                .unwrap(),
        };
        let tx2 = MessageWithSignature {
            message: Message::Legacy {
                chain_id: None,
                nonce: 2,
                gas_price: 30_000.as_u256(),
                gas_limit: 1_000_000,
                action: TransactionAction::Create,
                value: 10.as_u256(),
                input: Bytes::new(),
            },
            signature: MessageSignature::new(true, H256::repeat_byte(6), H256::repeat_byte(9))
                .unwrap(),
        };
        let txs = [tx1, tx2];

        let sender1 = Address::random();
        let sender2 = Address::random();
        let senders = [sender1, sender2];

        let block1_hash = H256::random();
        let body = BodyForStorage {
            base_tx_id: 1.into(),
            tx_amount: 2,
            uncles: vec![],
        };

        let db = new_mem_database().unwrap();
        let rwtx = db.begin_mutable().unwrap();
        let rwtx = &rwtx;

        storage_body::write(rwtx, block1_hash, 1, &body).unwrap();
        rwtx.set(tables::CanonicalHeader, 1.into(), block1_hash)
            .unwrap();
        tx::write(rwtx, 1, &txs).unwrap();
        tx_sender::write(rwtx, block1_hash, 1, senders.to_vec()).unwrap();

        let recovered_body = storage_body::read(rwtx, block1_hash, 1)
            .unwrap()
            .expect("Could not recover storage body.");
        let recovered_hash = rwtx
            .get(tables::CanonicalHeader, 1.into())
            .unwrap()
            .expect("Could not recover block hash");
        let recovered_txs = tx::read(rwtx, 1, 2).unwrap();
        let recovered_senders = tx_sender::read(rwtx, block1_hash, 1).unwrap();

        assert_eq!(body, recovered_body);
        assert_eq!(block1_hash, recovered_hash);
        assert_eq!(txs, *recovered_txs);
        assert_eq!(senders, *recovered_senders);
    }
}
