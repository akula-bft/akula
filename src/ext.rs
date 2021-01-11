use crate::{dbutils::*, models::*, Cursor, Transaction};
use anyhow::{bail, Context};
use arrayref::array_ref;
use async_stream::try_stream;
use async_trait::async_trait;
use bytes::Bytes;
use ethereum::Header;
use ethereum_types::{Address, H256, U256};
use futures::stream::BoxStream;
use mem::size_of;
use std::{
    collections::{HashMap, HashSet},
    mem,
};
use tokio::pin;
use tokio_stream::StreamExt;
use tracing::*;

#[async_trait]
pub trait TransactionExt: Transaction {
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

    async fn read_account_state(
        &self,
        block: H256,
        address: Address,
    ) -> anyhow::Result<Option<Account>> {
        if let Some(block_number) = self.get_block_number(block).await? {
            return Ok(self
                .state_reader(block_number)
                .read_account_data(address)
                .await?);
        }

        Ok(None)
    }

    fn state_reader(&self, block_nr: u64) -> StateReader<'_, Self> {
        StateReader {
            account_reads: Default::default(),
            storage_reads: Default::default(),
            code_reads: Default::default(),
            block_nr,
            tx: self,
        }
    }

    async fn get_as_of(&self, storage: bool, key: &[u8], timestamp: u64) -> anyhow::Result<Bytes> {
        // var dat []byte
        // v, err := FindByHistory(tx, storage, key, timestamp)
        // if err == nil {
        // 	dat = make([]byte, len(v))
        // 	copy(dat, v)
        // 	return dat, nil
        // }
        // if !errors.Is(err, ethdb.ErrKeyNotFound) {
        // 	return nil, err
        // }
        // v, err = tx.GetOne(dbutils.PlainStateBucket, key)
        // if err != nil {
        // 	return nil, err
        // }
        // if v == nil {
        // 	return nil, ethdb.ErrKeyNotFound
        // }
        // dat = make([]byte, len(v))
        // copy(dat, v)
        // return dat, nil
        todo!()
    }

    // pub async fn find_by_history(&self, storage: bool, key: &[u8], timestamp: u64) -> anyhow::Result<Option<Bytes>> {
    //     let hBucket;
    //     if storage {
    //         hBucket = Bucket::StorageHistory;
    //     } else {
    //         hBucket = Bucket::AccountsHistory;
    //     }

    //     let mut ch = self.cursor(hBucket).await?;
    //     let (k, v) = ch.seek(index_chunk_key(key, timestamp)).await?;

    //     if k.is_empty() {
    //         return Ok(None);
    //     }

    //     if (storage && k[..Address::len_bytes()] != key[..Address::len_bytes()] ||
    //             k[Address::len_bytes()..Address::len_bytes()+H256::len_bytes()] != key[Address::len_bytes()+/* common.IncarnationLength */size_of::<u64>()..]) || !k.starts_with(key) {
    //             return Ok(None);
    //     }
    //     let change_set_block = RoaringTreemap::deserialize_from(v)?.into_iter().find(|n| n >= timestamp);

    //     if let Some(change_set_block) = change_set_block {
    //         let cs_bucket = change_set_by_index_bucket(storage).0;
    //         let c = self.cursor_dup_sort(cs_bucket);

    //         if storage {
    //             data, err = changeset.Mapper[csBucket].WalkerAdapter(c).(changeset.StorageChangeSetPlain).FindWithIncarnation(changeSetBlock, key)
    //         } else {
    //             data, err = changeset.Mapper[csBucket].WalkerAdapter(c).Find(changeSetBlock, key)
    //         }
    //         if err != nil {
    //             if !errors.Is(err, changeset.ErrNotFound) {
    //                 return nil, fmt.Errorf("finding %x in the changeset %d: %w", key, changeSetBlock, err)
    //             }
    //             return nil, ethdb.ErrKeyNotFound
    //         }
    //     } else {
    //         return Ok(None);
    //     }

    //     //restore codehash
    //     if !storage {
    //         let acc = Account::decode_for_storage(data)?;
    //         if acc.Incarnation > 0 && acc.IsEmptyCodeHash() {
    //             var codeHash []byte
    //             var err error
    //             codeHash, err = tx.GetOne(dbutils.PlainContractCodeBucket, dbutils.PlainGenerateStoragePrefix(key, acc.Incarnation))
    //             if err != nil {
    //                 return nil, err
    //             }
    //             if len(codeHash) > 0 {
    //                 acc.CodeHash = common.BytesToHash(codeHash)
    //             }
    //             data = make([]byte, acc.EncodingLengthForStorage())
    //             acc.EncodeForStorage(data)
    //         }
    //         return data, nil
    //     }

    //     return data, nil
    // }
}

impl<T: ?Sized> TransactionExt for T where T: Transaction {}

fn walk_continue<K: AsRef<[u8]>>(
    k: &[u8],
    fixed_bytes: u64,
    fixed_bits: u64,
    start_key: &K,
    mask: u8,
) -> bool {
    !k.is_empty()
        && k.len() as u64 >= fixed_bytes
        && (fixed_bits == 0
            || (k[..fixed_bytes as usize - 1] == start_key.as_ref()[..fixed_bytes as usize - 1])
                && (k[fixed_bytes as usize - 1] & mask)
                    == (start_key.as_ref()[fixed_bytes as usize - 1] & mask))
}

#[async_trait]
pub trait CursorExt: Cursor {
    fn walk<'cur>(
        &'cur mut self,
        start_key: &'cur [u8],
        fixed_bits: u64,
    ) -> BoxStream<'cur, anyhow::Result<(Bytes, Bytes)>> {
        Box::pin(try_stream! {
            let (fixed_bytes, mask) = bytes_mask(fixed_bits);

            let (mut k, mut v) = self.seek(start_key).await?;

            while walk_continue(&k, fixed_bytes, fixed_bits, &start_key, mask) {
                yield (k, v);

                let next = self.next().await?;
                k = next.0;
                v = next.1;
            }
        })
    }
}

impl<T: ?Sized> CursorExt for T where T: Cursor {}

pub struct StateReader<'tx, Tx: ?Sized> {
    account_reads: HashSet<Address>,
    storage_reads: HashMap<Address, HashSet<H256>>,
    code_reads: HashSet<Address>,
    block_nr: u64,
    tx: &'tx Tx,
    // storage: HashMap<Address, LLRB>,
}

impl<'tx, Tx: ?Sized> StateReader<'tx, Tx> {
    pub async fn read_account_data(&mut self, address: Address) -> anyhow::Result<Option<Account>> {
        self.account_reads.insert(address);

        todo!()
    }
}
