use std::mem::size_of;

pub use super::*;

#[async_trait]
impl ChangeSetTable for tables::AccountChangeSet {
    const TEMPLATE: &'static str = "acc-ind-";

    type Key = common::Address;
    type IndexChunkKey = [u8; size_of::<Self::Key>() + common::BLOCK_NUMBER_LENGTH];
    type IndexTable = tables::AccountHistory;
    type EncodedStream<'tx: 'cs, 'cs> = impl EncodedStream<'tx, 'cs>;

    fn index_chunk_key(key: Self::Key, block_number: u64) -> Self::IndexChunkKey {
        let mut v = Self::IndexChunkKey::default();
        v[..key.as_ref().len()].copy_from_slice(key.as_ref());
        v[key.as_ref().len()..].copy_from_slice(&block_number.to_be_bytes());
        v
    }
    async fn find<'tx, C>(
        cursor: &mut C,
        block_number: u64,
        key: &Self::Key,
    ) -> anyhow::Result<Option<Bytes<'tx>>>
    where
        C: CursorDupSort<'tx, Self>,
        Self: Sized,
    {
        let k = dbutils::encode_block_number(block_number);
        if let Some(v) = cursor.seek_both_range(&k, key.as_bytes()).await? {
            let (_, k, v) = Self::decode(k.to_vec().into(), v);

            if k == *key {
                return Ok(Some(v));
            }
        }

        Ok(None)
    }

    fn encode<'cs, 'tx: 'cs>(
        block_number: u64,
        s: &'cs ChangeSet<'tx, Self::Key>,
    ) -> Self::EncodedStream<'tx, 'cs> {
        let k = dbutils::encode_block_number(block_number);

        s.iter().map(move |cs| {
            let mut new_v = vec![0; cs.key.as_ref().len() + cs.value.len()];
            new_v[..cs.key.as_ref().len()].copy_from_slice(cs.key.as_ref());
            new_v[cs.key.as_ref().len()..].copy_from_slice(&*cs.value);

            (Bytes::from(k.to_vec()), new_v.into())
        })
    }

    fn decode<'tx>(db_key: Bytes<'tx>, db_value: Bytes<'tx>) -> (u64, Self::Key, Bytes<'tx>) {
        let block_n = u64::from_be_bytes(*array_ref!(db_key, 0, common::BLOCK_NUMBER_LENGTH));

        let mut k = db_value;
        let v = k.split_off(common::ADDRESS_LENGTH);

        (block_n, common::Address::from_slice(&k), v)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ethereum_types::Address;

    #[test]
    fn account_encoding() {
        type Table = tables::AccountChangeSet;

        let mut ch = ChangeSet::default();

        for (i, val) in vec![
            "f7f6db1eb17c6d582078e0ffdd0c".into(),
            "b1e9b5c16355eede662031dd621d08faf4ea".into(),
            "862cf52b74f1cea41ddd8ffa4b3e7c7790".into(),
        ]
        .into_iter()
        .enumerate()
        {
            let address = format!("0xBe828AD8B538D1D691891F6c725dEdc5989abBc{}", i)
                .parse::<Address>()
                .unwrap();

            ch.insert(Change::new(address, val));
        }

        let mut ch2 = ChangeSet::default();

        for (k, v) in Table::encode(1, &ch) {
            let (_, k, v) = Table::decode(k, v);

            ch2.insert(Change::new(k, v));
        }

        assert_eq!(ch, ch2);
    }
}
