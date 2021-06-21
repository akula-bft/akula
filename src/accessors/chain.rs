use crate::{
    common,
    dbutils::*,
    kv::{tables, traits::MutableCursor},
    models::*,
    txdb, MutableTransaction, Transaction,
};
use anyhow::{bail, Context};
use arrayref::array_ref;
use ethereum::Header as HeaderType;
use ethereum_types::{Address, H256, U256};
use tokio::pin;
use tokio_stream::StreamExt;
use tracing::*;

pub mod canonical_hash {
    use super::*;

    pub async fn read<'db: 'tx, 'tx, Tx: Transaction<'db>>(
        tx: &'tx Tx,
        block_num: u64,
    ) -> anyhow::Result<Option<H256>> {
        let key = encode_block_number(block_num);

        trace!(
            "Reading canonical hash of {} from at {}",
            block_num,
            hex::encode(&key)
        );

        if let Some(b) = tx.get(&tables::CanonicalHeader, &key).await? {
            match b.len() {
                common::HASH_LENGTH => return Ok(Some(H256::from_slice(&*b))),
                other => bail!("invalid length: {}", other),
            }
        }

        Ok(None)
    }

    pub async fn write<'db: 'tx, 'tx, RwTx: MutableTransaction<'db>>(
        tx: &'tx RwTx,
        block_num: u64,
        hash: H256,
    ) -> anyhow::Result<()> {
        let key = encode_block_number(block_num);

        trace!("Writing canonical hash of {}", block_num);

        let mut cursor = tx.mutable_cursor(&tables::CanonicalHeader).await?;
        cursor.put(&key, hash.as_bytes()).await.unwrap();

        Ok(())
    }
}

pub mod header_number {
    use super::*;

    pub async fn read<'db: 'tx, 'tx, Tx: Transaction<'db>>(
        tx: &'tx Tx,
        hash: H256,
    ) -> anyhow::Result<Option<u64>> {
        trace!("Reading block number for hash {:?}", hash);

        if let Some(b) = tx
            .get(&tables::HeaderNumber, &hash.to_fixed_bytes())
            .await?
        {
            match b.len() {
                common::BLOCK_NUMBER_LENGTH => {
                    return Ok(Some(u64::from_be_bytes(*array_ref![b, 0, 8])))
                }
                other => bail!("invalid length: {}", other),
            }
        }

        Ok(None)
    }
}

pub mod header {
    use super::*;

    pub async fn read<'db: 'tx, 'tx, Tx: Transaction<'db>>(
        tx: &'tx Tx,
        hash: H256,
        number: u64,
    ) -> anyhow::Result<Option<HeaderType>> {
        trace!("Reading header for block {}/{:?}", number, hash);

        if let Some(b) = tx.get(&tables::Header, &header_key(number, hash)).await? {
            return Ok(Some(rlp::decode(&b)?));
        }

        Ok(None)
    }
}

pub mod tx {
    use super::*;

    pub async fn read<'db: 'tx, 'tx, Tx: Transaction<'db>>(
        tx: &'tx Tx,
        base_tx_id: u64,
        amount: u32,
    ) -> anyhow::Result<Vec<ethereum::Transaction>> {
        trace!(
            "Reading {} transactions starting from {}",
            amount,
            base_tx_id
        );

        Ok(if amount > 0 {
            let mut out = Vec::with_capacity(amount as usize);

            let mut cursor = tx.cursor(&tables::BlockTransaction).await?;

            let start_key = base_tx_id.to_be_bytes();
            let walker = txdb::walk(&mut cursor, &start_key, 0);

            pin!(walker);

            while let Some((_, tx_rlp)) = walker.try_next().await? {
                out.push(rlp::decode(&tx_rlp).context("broken tx rlp")?);

                if out.len() >= amount as usize {
                    break;
                }
            }

            out
        } else {
            vec![]
        })
    }

    pub async fn write<'db: 'tx, 'tx, RwTx: MutableTransaction<'db>>(
        tx: &'tx RwTx,
        base_tx_id: u64,
        txs: &[ethereum::Transaction],
    ) -> anyhow::Result<()> {
        trace!(
            "Writing {} transactions starting from {}",
            txs.len(),
            base_tx_id
        );

        let mut cursor = tx.mutable_cursor(&tables::BlockTransaction).await.unwrap();

        for (i, eth_tx) in txs.iter().enumerate() {
            let key = (base_tx_id + i as u64).to_be_bytes();
            let data = rlp::encode(eth_tx);
            cursor.put(&key, &data).await.unwrap();
        }

        Ok(())
    }
}

pub mod tx_sender {
    use super::*;

    pub async fn read<'db: 'tx, 'tx, Tx: Transaction<'db>>(
        tx: &'tx Tx,
        base_tx_id: u64,
        amount: u32,
    ) -> anyhow::Result<Vec<Address>> {
        trace!(
            "Reading {} transaction senders starting from {}",
            amount,
            base_tx_id
        );

        Ok(if amount > 0 {
            let mut cursor = tx.cursor(&tables::TxSender).await?;

            let start_key = base_tx_id.to_be_bytes();
            txdb::walk(&mut cursor, &start_key, 0)
                .take(amount as usize)
                .map(|res| res.map(|(_, address_bytes)| Address::from_slice(&*address_bytes)))
                .collect::<anyhow::Result<Vec<_>>>()
                .await?
        } else {
            vec![]
        })
    }

    pub async fn write<'db: 'tx, 'tx, RwTx: MutableTransaction<'db>>(
        tx: &'tx RwTx,
        base_tx_id: u64,
        senders: &[Address],
    ) -> anyhow::Result<()> {
        trace!(
            "Writing {} transaction senders starting from {}",
            senders.len(),
            base_tx_id
        );

        let mut cursor = tx.mutable_cursor(&tables::TxSender).await.unwrap();

        for (i, sender) in senders.iter().enumerate() {
            let key = (base_tx_id + i as u64).to_be_bytes();
            let data = sender.to_fixed_bytes();
            cursor.put(&key, &data).await.unwrap();
        }

        Ok(())
    }
}

pub mod storage_body {
    use bytes::Bytes;

    use super::*;

    async fn read_raw<'db: 'tx, 'tx, Tx: Transaction<'db>>(
        tx: &'tx Tx,
        hash: H256,
        number: u64,
    ) -> anyhow::Result<Option<Bytes<'tx>>> {
        trace!("Reading storage body for block {}/{:?}", number, hash);

        if let Some(b) = tx
            .get(&tables::BlockBody, &header_key(number, hash))
            .await?
        {
            return Ok(Some(b));
        }

        Ok(None)
    }

    pub async fn read<'db: 'tx, 'tx, Tx: Transaction<'db>>(
        tx: &'tx Tx,
        hash: H256,
        number: u64,
    ) -> anyhow::Result<Option<BodyForStorage>> {
        if let Some(b) = read_raw(tx, hash, number).await? {
            return Ok(Some(rlp::decode(&b)?));
        }

        Ok(None)
    }

    pub async fn has<'db: 'tx, 'tx, Tx: Transaction<'db>>(
        tx: &'tx Tx,
        hash: H256,
        number: u64,
    ) -> anyhow::Result<bool> {
        Ok(read_raw(tx, hash, number).await?.is_some())
    }

    pub async fn write<'db: 'tx, 'tx, RwTx: MutableTransaction<'db>>(
        tx: &'tx RwTx,
        hash: H256,
        number: u64,
        body: &BodyForStorage,
    ) -> anyhow::Result<()> {
        trace!("Writing storage body for block {}/{:?}", number, hash);

        let data = rlp::encode(body);
        let mut cursor = tx.mutable_cursor(&tables::BlockBody).await.unwrap();
        cursor.put(&header_key(number, hash), &data).await.unwrap();

        Ok(())
    }
}

pub mod td {
    use super::*;

    pub async fn read<'db: 'tx, 'tx, Tx: Transaction<'db>>(
        tx: &'tx Tx,
        hash: H256,
        number: u64,
    ) -> anyhow::Result<Option<U256>> {
        trace!("Reading total difficulty at block {}/{:?}", number, hash);

        if let Some(b) = tx
            .get(&tables::HeadersTotalDifficulty, &header_key(number, hash))
            .await?
        {
            trace!("Reading TD RLP: {}", hex::encode(&b));

            return Ok(Some(rlp::decode(&b)?));
        }

        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kv::{new_mem_database, traits::MutableKV};

    #[tokio::test]
    async fn accessors() {
        let tx1 = ethereum::Transaction {
            nonce: 1.into(),
            gas_price: 20_000.into(),
            gas_limit: 3_000_000.into(),
            action: ethereum::TransactionAction::Create,
            value: 0.into(),
            input: vec![],
            signature: ethereum::TransactionSignature::new(
                27,
                H256::repeat_byte(2),
                H256::repeat_byte(3),
            )
            .unwrap(),
        };
        let tx2 = ethereum::Transaction {
            nonce: 2.into(),
            gas_price: 30_000.into(),
            gas_limit: 1_000_000.into(),
            action: ethereum::TransactionAction::Create,
            value: 10.into(),
            input: vec![],
            signature: ethereum::TransactionSignature::new(
                28,
                H256::repeat_byte(6),
                H256::repeat_byte(9),
            )
            .unwrap(),
        };
        let txs = [tx1, tx2];

        let sender1 = Address::random();
        let sender2 = Address::random();
        let senders = [sender1, sender2];

        let block1_hash = H256::random();
        let body = BodyForStorage {
            base_tx_id: 1,
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
        tx_sender::write(rwtx, 1, &senders).await.unwrap();

        let recovered_body = storage_body::read(rwtx, block1_hash, 1)
            .await
            .unwrap()
            .expect("Could not recover storage body.");
        let recovered_hash = canonical_hash::read(rwtx, 1)
            .await
            .unwrap()
            .expect("Could not recover block hash");
        let recovered_txs = tx::read(rwtx, 1, 2).await.unwrap();
        let recovered_senders = tx_sender::read(rwtx, 1, 2).await.unwrap();

        assert_eq!(body, recovered_body);
        assert_eq!(block1_hash, recovered_hash);
        assert_eq!(txs, *recovered_txs);
        assert_eq!(senders, *recovered_senders);
    }
}
