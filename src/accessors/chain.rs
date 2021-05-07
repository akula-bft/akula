use crate::{common, dbutils::*, models::*, tables, txdb, Transaction};
use anyhow::{bail, Context};
use arrayref::array_ref;
use ethereum::Header;
use ethereum_types::{H256, U256};
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

        if let Some(b) = tx.get::<tables::HeaderCanonical>(&key).await? {
            match b.len() {
                common::HASH_LENGTH => return Ok(Some(H256::from_slice(&*b))),
                other => bail!("invalid length: {}", other),
            }
        }

        Ok(None)
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
            .get::<tables::HeaderNumber>(&hash.to_fixed_bytes())
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
    ) -> anyhow::Result<Option<Header>> {
        trace!("Reading header for block {}/{:?}", number, hash);

        if let Some(b) = tx.get::<tables::Headers>(&header_key(number, hash)).await? {
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

            let mut cursor = tx.cursor::<tables::EthTx>().await?;

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
            .get::<tables::BlockBody>(&header_key(number, hash))
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
}

pub mod td {
    use super::*;

    pub async fn read<'db: 'tx, 'tx, Tx: Transaction<'db>>(
        tx: &'tx Tx,
        hash: H256,
        number: u64,
    ) -> anyhow::Result<Option<U256>> {
        trace!("Reading totatl difficulty at block {}/{:?}", number, hash);

        if let Some(b) = tx
            .get::<tables::HeaderTD>(&header_key(number, hash))
            .await?
        {
            trace!("Reading TD RLP: {}", hex::encode(&b));

            return Ok(Some(rlp::decode(&b)?));
        }

        Ok(None)
    }
}
