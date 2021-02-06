use std::marker::PhantomData;

use self::account_utils::find_in_account_changeset;
pub use super::*;
use crate::CursorDupSort;
use async_trait::async_trait;

pub trait EncodedStream = Iterator<Item = (Bytes<'static>, Bytes<'static>)>;

pub struct AccountChangeSetPlain<
    'cur,
    'tx: 'cur,
    C: CursorDupSort<'tx, buckets::PlainAccountChangeSet>,
> {
    pub c: &'cur mut C,
    _marker: PhantomData<&'tx ()>,
}

impl<'cur, 'tx: 'cur, C: CursorDupSort<'tx, buckets::PlainAccountChangeSet>>
    AccountChangeSetPlain<'cur, 'tx, C>
{
    pub fn new(c: &'cur mut C) -> Self {
        Self {
            c,
            _marker: PhantomData,
        }
    }
}

#[async_trait(?Send)]
impl<'cur, 'tx: 'cur, C: 'cur + CursorDupSort<'tx, buckets::PlainAccountChangeSet>> Walker
    for AccountChangeSetPlain<'cur, 'tx, C>
{
    type Key = [u8; common::ADDRESS_LENGTH];
    type WalkStream<'w> = impl WalkStream<Self::Key>;

    fn walk(&mut self, from: u64, to: u64) -> Self::WalkStream<'_> {
        super::storage_utils::walk(
            &mut self.c,
            |db_key, db_value| {
                let (b, k1, v) = from_account_db_format(db_key, db_value);
                let mut k = [0; common::ADDRESS_LENGTH];
                k[..].copy_from_slice(&k1[..]);
                (b, k, v)
            },
            from,
            to,
        )
    }

    async fn find(
        &mut self,
        block_number: u64,
        k: &Self::Key,
    ) -> anyhow::Result<Option<Bytes<'static>>> {
        find_in_account_changeset(&mut self.c, block_number, k).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes_literal::bytes;
    use ethereum_types::Address;

    #[test]
    fn account_encoding() {
        type Bucket = buckets::PlainAccountChangeSet;

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

            ch.insert(Change::new(address.to_fixed_bytes(), val));
        }

        let mut ch2 = ChangeSet::default();

        for (k, v) in Bucket::encode(1, &ch) {
            let (_, k, v) = Bucket::decode(k, v);

            ch2.insert(Change::new(k, v));
        }

        assert_eq!(ch, ch2);
    }
}
