use crate::{buckets, common, dbutils::*, models::*, txdb, txutil, Transaction};
use anyhow::{bail, Context};
use arrayref::array_ref;
use ethereum::Header;
use ethereum_types::{H256, U256};
use tokio::pin;
use tokio_stream::StreamExt;
use tracing::*;

pub mod canonical_hash {
    use super::*;

    pub async fn read<Tx: Transaction>(tx: &Tx, block_num: u64) -> anyhow::Result<Option<H256>> {
        let key = header_hash_key(block_num);

        trace!(
            "Reading canonical hash of {} from at {}",
            block_num,
            hex::encode(&key)
        );

        let b = txutil::get_one::<_, buckets::Header>(tx, &key).await?;

        match b.len() {
            0 => Ok(None),
            common::HASH_LENGTH => Ok(Some(H256::from_slice(&*b))),
            other => bail!("invalid length: {}", other),
        }
    }
}

pub mod header_number {
    use super::*;

    pub async fn read<Tx: Transaction>(tx: &Tx, hash: H256) -> anyhow::Result<Option<u64>> {
        trace!("Reading block number for hash {:?}", hash);

        let b = txutil::get_one::<_, buckets::HeaderNumber>(tx, &hash.to_fixed_bytes()).await?;

        match b.len() {
            0 => Ok(None),
            common::BLOCK_NUMBER_LENGTH => Ok(Some(u64::from_be_bytes(*array_ref![b, 0, 8]))),
            other => bail!("invalid length: {}", other),
        }
    }
}

pub mod header {
    use super::*;

    pub async fn read<Tx: Transaction>(
        tx: &Tx,
        hash: H256,
        number: u64,
    ) -> anyhow::Result<Option<Header>> {
        trace!("Reading header for block {}/{:?}", number, hash);

        let b = txutil::get_one::<_, buckets::Header>(tx, &number_hash_composite_key(number, hash))
            .await?;

        if b.is_empty() {
            return Ok(None);
        }

        Ok(Some(rlp::decode(&b)?))
    }
}

pub mod tx {
    use super::*;

    pub async fn read<Tx: Transaction>(
        tx: &Tx,
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

            let mut cursor = tx.cursor::<buckets::EthTx>().await?;

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

pub mod body {
    use super::*;

    pub async fn read<Tx: Transaction>(
        tx: &Tx,
        hash: H256,
        number: u64,
    ) -> anyhow::Result<Option<BodyForStorage>> {
        trace!("Reading storage body for block {}/{:?}", number, hash);

        let b =
            txutil::get_one::<_, buckets::BlockBody>(tx, &number_hash_composite_key(number, hash))
                .await?;

        if b.is_empty() {
            return Ok(None);
        }

        Ok(rlp::decode(&b)?)
    }
}

pub mod td {
    use super::*;

    pub async fn read<Tx: Transaction>(
        tx: &Tx,
        hash: H256,
        number: u64,
    ) -> anyhow::Result<Option<U256>> {
        trace!("Reading totatl difficulty at block {}/{:?}", number, hash);

        let b = txutil::get_one::<_, buckets::Header>(tx, &header_td_key(number, hash)).await?;

        if b.is_empty() {
            return Ok(None);
        }

        trace!("Reading TD RLP: {}", hex::encode(&b));

        Ok(Some(rlp::decode(&b)?))
    }
}
