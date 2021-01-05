use crate::{dbutils::*, models::*, Cursor, Transaction};
use anyhow::{bail, Context};
use arrayref::array_ref;
use async_trait::async_trait;
use bytes::Bytes;
use ethereum::Header;
use ethereum_types::{H256, U256};
use mem::size_of;
use std::mem;
use tokio::pin;
use tokio_stream::StreamExt;
use tracing::*;

#[async_trait]
pub trait TransactionExt: Transaction {
    async fn get_one<'a>(&'a self, bucket_name: &'a str, key: &'a [u8]) -> anyhow::Result<Bytes>
    where
        Self: Sync,
    {
        let mut cursor = self.cursor(bucket_name).await?;

        Ok(cursor.seek_exact(key).await?.1)
    }

    async fn read_canonical_hash(&self, block_num: u64) -> anyhow::Result<Option<H256>> {
        let key = header_hash_key(block_num);

        trace!(
            "Reading canonical hash of {} from bucket {} at {}",
            block_num,
            HEADER_PREFIX,
            hex::encode(&key)
        );

        let b = self.get_one(HEADER_PREFIX, &key).await?;

        const L: usize = H256::len_bytes();

        match b.len() {
            0 => Ok(None),
            L => Ok(Some(H256::from_slice(&*b))),
            other => bail!("invalid length: {}", other),
        }
    }

    async fn read_header(&self, hash: H256, number: u64) -> anyhow::Result<Option<Header>> {
        trace!("Reading header for block {}/{:?}", number, hash);

        let b = self
            .get_one(HEADER_PREFIX, &number_hash_composite_key(number, hash))
            .await?;

        if b.is_empty() {
            return Ok(None);
        }

        Ok(Some(rlp::decode(&b)?))
    }

    async fn get_block_number(&self, hash: H256) -> anyhow::Result<Option<u64>> {
        trace!("Reading block number for hash {:?}", hash);

        let b = self
            .get_one(HEADER_NUMBER_PREFIX, &hash.to_fixed_bytes())
            .await?;

        const L: usize = size_of::<u64>();

        match b.len() {
            0 => Ok(None),
            L => Ok(Some(u64::from_be_bytes(*array_ref![b, 0, 8]))),
            other => bail!("invalid length: {}", other),
        }
    }

    async fn read_chain_config(&self, block: H256) -> anyhow::Result<Option<ChainConfig>> {
        let key = block.as_bytes();

        trace!(
            "Reading chain config for block {:?} from bucket {} at key {}",
            block,
            CONFIG_PREFIX,
            hex::encode(&key)
        );

        let b = self.get_one(CONFIG_PREFIX, &key).await?;

        trace!("Read chain config data: {}", hex::encode(&b));

        if b.is_empty() {
            return Ok(None);
        }

        Ok(Some(serde_json::from_slice(&*b).context("invalid JSON")?))
    }

    async fn get_stage_progress(&self, stage: SyncStage) -> anyhow::Result<Option<u64>> {
        trace!("Reading stage {:?} progress", stage);

        let b = self.get_one(SYNC_STAGE_PROGRESS, stage.as_ref()).await?;

        if b.is_empty() {
            return Ok(None);
        }

        let block_num_byte_len = mem::size_of::<u64>();

        Ok(Some(u64::from_be_bytes(*array_ref![
            b.get(0..block_num_byte_len)
                .context("failed to read block number from bytes")?,
            0,
            mem::size_of::<u64>()
        ])))
    }

    async fn get_storage_body(
        &self,
        hash: H256,
        number: u64,
    ) -> anyhow::Result<Option<BodyForStorage>> {
        trace!("Reading storage body for block {}/{:?}", number, hash);

        let b = self
            .get_one(BLOCK_BODY_PREFIX, &number_hash_composite_key(number, hash))
            .await?;

        if b.is_empty() {
            return Ok(None);
        }

        Ok(rlp::decode(&b)?)
    }

    async fn read_transactions(
        &self,
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

            let mut cursor = self.cursor(ETH_TX).await?;

            let start_key = base_tx_id.to_be_bytes();
            let walker = cursor.walk(&start_key, 0);

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

    async fn get_total_difficulty(&self, hash: H256, number: u64) -> anyhow::Result<Option<U256>> {
        trace!("Reading totatl difficulty at block {}/{:?}", number, hash);

        let b = self
            .get_one(HEADER_PREFIX, &header_td_key(number, hash))
            .await?;

        if b.is_empty() {
            return Ok(None);
        }

        trace!("Reading TD RLP: {}", hex::encode(&b));

        Ok(Some(rlp::decode(&b)?))
    }
}

impl<Tx: ?Sized> TransactionExt for Tx where Tx: Transaction {}
