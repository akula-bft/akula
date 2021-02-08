use std::marker::PhantomData;

use self::account_utils::find_in_account_changeset;
pub use super::*;
use crate::CursorDupSort;
use async_trait::async_trait;

pub trait EncodedStream<'tx, 'cs> = Iterator<Item = (Bytes<'tx>, Bytes<'tx>)> + 'cs;

pub struct AccountChangeSetPlain<'tx, C: CursorDupSort<'tx, buckets::PlainAccountChangeSet>> {
    pub c: C,
    _marker: PhantomData<&'tx ()>,
}

impl<'tx, C: CursorDupSort<'tx, buckets::PlainAccountChangeSet>> AccountChangeSetPlain<'tx, C> {
    pub fn new(c: C) -> Self {
        Self {
            c,
            _marker: PhantomData,
        }
    }
}

#[async_trait(?Send)]
impl<'tx, C: CursorDupSort<'tx, buckets::PlainAccountChangeSet>> Walker<'tx>
    for AccountChangeSetPlain<'tx, C>
{
    type Key = [u8; common::ADDRESS_LENGTH];

    async fn find(
        &mut self,
        block_number: u64,
        k: &Self::Key,
    ) -> anyhow::Result<Option<Bytes<'tx>>> {
        find_in_account_changeset(&mut self.c, block_number, k).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
