use crate::{
    kv::{tables, traits::*},
    models::*,
};
use tokio_stream::StreamExt;
use tracing::*;

pub mod canonical_hash {
    use super::*;

    pub async fn read<'db, Tx: Transaction<'db>>(
        tx: &Tx,
        block_number: impl Into<BlockNumber>,
    ) -> anyhow::Result<Option<H256>> {
        tx.get(tables::CanonicalHeader, block_number.into()).await
    }

    pub async fn write<'db, RwTx: MutableTransaction<'db>>(
        tx: &RwTx,
        block_number: impl Into<BlockNumber>,
        hash: H256,
    ) -> anyhow::Result<()> {
        let block_number = block_number.into();

        trace!("Writing canonical hash of {}", block_number);

        let mut cursor = tx.mutable_cursor(tables::CanonicalHeader).await?;
        cursor.put(block_number, hash).await.unwrap();

        Ok(())
    }
}

pub mod header_number {
    use super::*;

    pub async fn read<'db, Tx: Transaction<'db>>(
        tx: &Tx,
        hash: H256,
    ) -> anyhow::Result<Option<BlockNumber>> {
        trace!("Reading block number for hash {:?}", hash);

        tx.get(tables::HeaderNumber, hash).await
    }
}

pub mod header {
    use super::*;

    pub async fn read<'db, Tx: Transaction<'db>>(
        tx: &Tx,
        hash: H256,
        number: impl Into<BlockNumber>,
    ) -> anyhow::Result<Option<BlockHeader>> {
        let number = number.into();
        trace!("Reading header for block {}/{:?}", number, hash);

        tx.get(tables::Header, (number, hash)).await
    }
}

pub mod tx {
    use super::*;

    pub async fn read<'db, Tx: Transaction<'db>>(
        tx: &Tx,
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
            walk(
                &mut tx.cursor(tables::BlockTransaction).await?,
                Some(base_tx_id),
            )
            .take(amount)
            .map(|res| res.map(|(_, v)| v))
            .collect()
            .await
        } else {
            Ok(vec![])
        }
    }

    pub async fn write<'db, RwTx: MutableTransaction<'db>>(
        tx: &RwTx,
        base_tx_id: impl Into<TxIndex>,
        txs: &[MessageWithSignature],
    ) -> anyhow::Result<()> {
        let base_tx_id = base_tx_id.into();
        trace!(
            "Writing {} transactions starting from {}",
            txs.len(),
            base_tx_id
        );

        let mut cursor = tx.mutable_cursor(tables::BlockTransaction).await.unwrap();

        for (i, eth_tx) in txs.iter().enumerate() {
            cursor
                .put(base_tx_id + i as u64, eth_tx.clone())
                .await
                .unwrap();
        }

        Ok(())
    }
}

pub mod tx_sender {
    use super::*;

    pub async fn read<'db, Tx: Transaction<'db>>(
        tx: &Tx,
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
            .get(tables::TxSender, (number, hash))
            .await?
            .unwrap_or_default())
    }

    pub async fn write<'db, RwTx: MutableTransaction<'db>>(
        tx: &RwTx,
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

        tx.set(tables::TxSender, (number, hash), senders)
            .await
            .unwrap();

        Ok(())
    }
}

pub mod storage_body {
    use super::*;

    pub async fn read<'db, Tx: Transaction<'db>>(
        tx: &Tx,
        hash: H256,
        number: impl Into<BlockNumber>,
    ) -> anyhow::Result<Option<BodyForStorage>> {
        let number = number.into();
        trace!("Reading storage body for block {}/{:?}", number, hash);

        tx.get(tables::BlockBody, (number, hash)).await
    }

    pub async fn has<'db, Tx: Transaction<'db>>(
        tx: &Tx,
        hash: H256,
        number: impl Into<BlockNumber>,
    ) -> anyhow::Result<bool> {
        Ok(read(tx, hash, number).await?.is_some())
    }

    pub async fn write<'db, RwTx: MutableTransaction<'db>>(
        tx: &RwTx,
        hash: H256,
        number: impl Into<BlockNumber>,
        body: &BodyForStorage,
    ) -> anyhow::Result<()> {
        let number = number.into();
        trace!("Writing storage body for block {}/{:?}", number, hash);

        tx.set(tables::BlockBody, (number, hash), body.clone())
            .await
            .unwrap();

        Ok(())
    }
}

pub mod block_body {
    use super::*;

    async fn read_base<'db, Tx: Transaction<'db>>(
        tx: &Tx,
        hash: H256,
        number: impl Into<BlockNumber>,
    ) -> anyhow::Result<Option<(BlockBody, TxIndex)>> {
        if let Some(body) = super::storage_body::read(tx, hash, number).await? {
            let transactions =
                super::tx::read(tx, body.base_tx_id, body.tx_amount.try_into()?).await?;

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

    pub async fn read_without_senders<'db, Tx: Transaction<'db>>(
        tx: &Tx,
        hash: H256,
        number: impl Into<BlockNumber>,
    ) -> anyhow::Result<Option<BlockBody>> {
        Ok(read_base(tx, hash, number).await?.map(|(v, _)| v))
    }

    pub async fn read_with_senders<'db, Tx: Transaction<'db>>(
        tx: &Tx,
        hash: H256,
        number: impl Into<BlockNumber>,
    ) -> anyhow::Result<Option<BlockBodyWithSenders>> {
        let number = number.into();
        if let Some((body, _)) = read_base(tx, hash, number).await? {
            let senders = super::tx_sender::read(tx, hash, number).await?;

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

    pub async fn read<'db, Tx: Transaction<'db>>(
        tx: &Tx,
        hash: H256,
        number: impl Into<BlockNumber>,
    ) -> anyhow::Result<Option<U256>> {
        let number = number.into();
        trace!("Reading total difficulty at block {}/{:?}", number, hash);

        tx.get(tables::HeadersTotalDifficulty, (number, hash)).await
    }
}

pub mod tl {
    use super::*;

    pub async fn read<'db, Tx: Transaction<'db>>(
        tx: &Tx,
        tx_hash: H256,
    ) -> anyhow::Result<Option<BlockNumber>> {
        trace!("Reading Block number for a tx_hash {:?}", tx_hash);

        Ok(tx
            .get(tables::BlockTransactionLookup, tx_hash)
            .await?
            .map(|b| b.0))
    }

    pub async fn write<'db: 'tx, 'tx, RwTx: MutableTransaction<'db>>(
        tx: &'tx RwTx,
        hashed_tx_data: H256,
        block_number: BlockNumber,
    ) -> anyhow::Result<()> {
        trace!("Writing tx_lookup for hash {}", hashed_tx_data);

        let mut cursor = tx
            .mutable_cursor(tables::BlockTransactionLookup)
            .await
            .unwrap();
        cursor
            .put(hashed_tx_data, block_number.into())
            .await
            .unwrap();

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kv::{new_mem_database, traits::MutableKV};
    use bytes::Bytes;

    #[tokio::test]
    async fn accessors() {
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
        let rwtx = db.begin_mutable().await.unwrap();
        let rwtx = &rwtx;

        storage_body::write(rwtx, block1_hash, 1, &body)
            .await
            .unwrap();
        canonical_hash::write(rwtx, 1, block1_hash).await.unwrap();
        tx::write(rwtx, 1, &txs).await.unwrap();
        tx_sender::write(rwtx, block1_hash, 1, senders.to_vec())
            .await
            .unwrap();

        let recovered_body = storage_body::read(rwtx, block1_hash, 1)
            .await
            .unwrap()
            .expect("Could not recover storage body.");
        let recovered_hash = canonical_hash::read(rwtx, 1)
            .await
            .unwrap()
            .expect("Could not recover block hash");
        let recovered_txs = tx::read(rwtx, 1, 2).await.unwrap();
        let recovered_senders = tx_sender::read(rwtx, block1_hash, 1).await.unwrap();

        assert_eq!(body, recovered_body);
        assert_eq!(block1_hash, recovered_hash);
        assert_eq!(txs, *recovered_txs);
        assert_eq!(senders, *recovered_senders);
    }
}
