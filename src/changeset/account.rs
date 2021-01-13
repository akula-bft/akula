use self::account_utils::{encode_accounts_2, find_in_account_changeset};
pub use super::*;
use crate::CursorDupSort;
use async_trait::async_trait;

pub type EncodedStream = Box<dyn Iterator<Item = (Bytes, Bytes)> + Send>;
pub type Encoder = fn(u64, ChangeSet) -> EncodedStream;
pub type Decoder = fn(Bytes, Bytes) -> (u64, Bytes, Bytes);

/* Hashed changesets (key is a hash of common.Address) */

impl ChangeSet {
    pub fn new_account() -> Self {
        Self {
            changes: vec![],
            key_len: common::HASH_LENGTH,
        }
    }
}

pub fn encode_accounts(block_n: u64, s: ChangeSet) -> EncodedStream {
    Box::new(encode_accounts_2(block_n, s))
}

pub struct AccountChangeSet<C: CursorDupSort> {
    pub c: C,
}

#[async_trait]
impl<C: CursorDupSort> Walker2 for AccountChangeSet<C> {
    fn walk(&mut self, from: u64, to: u64) -> BoxStream<'_, anyhow::Result<(u64, Bytes, Bytes)>> {
        super::storage_utils::walk(&mut self.c, from, to, common::HASH_LENGTH)
    }

    fn walk_reverse(
        &mut self,
        from: u64,
        to: u64,
    ) -> BoxStream<'_, anyhow::Result<(u64, Bytes, Bytes)>> {
        super::storage_utils::walk_reverse(&mut self.c, from, to, common::HASH_LENGTH)
    }

    async fn find(&mut self, block_number: u64, k: &[u8]) -> anyhow::Result<Option<Bytes>> {
        find_in_account_changeset(&mut self.c, block_number, k, common::HASH_LENGTH).await
    }
}

/* Plain changesets (key is a common.Address) */

impl ChangeSet {
    pub fn new_account_plain() -> Self {
        Self {
            changes: vec![],
            key_len: common::ADDRESS_LENGTH,
        }
    }
}

#[allow(non_upper_case_globals)]
pub const encode_accounts_plain: Encoder = encode_accounts;

pub struct AccountChangeSetPlain<C: CursorDupSort> {
    pub c: C,
}

#[async_trait]
impl<C: CursorDupSort> Walker2 for AccountChangeSetPlain<C> {
    fn walk(&mut self, from: u64, to: u64) -> BoxStream<'_, anyhow::Result<(u64, Bytes, Bytes)>> {
        super::storage_utils::walk(&mut self.c, from, to, common::ADDRESS_LENGTH)
    }

    fn walk_reverse(
        &mut self,
        from: u64,
        to: u64,
    ) -> BoxStream<'_, anyhow::Result<(u64, Bytes, Bytes)>> {
        super::storage_utils::walk_reverse(&mut self.c, from, to, common::ADDRESS_LENGTH)
    }

    async fn find(&mut self, block_number: u64, k: &[u8]) -> anyhow::Result<Option<Bytes>> {
        find_in_account_changeset(&mut self.c, block_number, k, common::ADDRESS_LENGTH).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dbutils;
    use bytes_literal::bytes;
    use ethereum_types::{Address, H256};
    use sha3::{Digest, Keccak256};

    #[test]
    fn encoding_account_hashed() {
        let m = &MAPPER[&dbutils::Bucket::AccountChangeSet];
        run_test_account_encoding(true, m.new, m.encode, &m.decode);
    }

    #[test]
    fn encoding_account_plain() {
        let m = &MAPPER[&dbutils::Bucket::PlainAccountChangeSet];
        run_test_account_encoding(false, m.new, m.encode, &m.decode)
    }

    #[tokio::main]
    async fn run_test_account_encoding<
        Decoder: Fn(Bytes, Bytes) -> (u64, Bytes, Bytes) + Send + Sync,
    >(
        is_hashed: bool,
        new: fn() -> ChangeSet,
        enc: Encoder,
        dec: Decoder,
    ) {
        let mut ch = (new)();

        for (i, val) in vec![
            bytes!["f7f6db1eb17c6d582078e0ffdd0c"],
            bytes!["b1e9b5c16355eede662031dd621d08faf4ea"],
            bytes!["862cf52b74f1cea41ddd8ffa4b3e7c7790"],
        ]
        .into_iter()
        .enumerate()
        {
            let address = format!("0xBe828AD8B538D1D691891F6c725dEdc5989abBc{}", i)
                .parse::<Address>()
                .unwrap();

            if is_hashed {
                let addr_hash = common::hash_data(address.as_bytes());
                ch.insert(addr_hash.as_bytes().to_vec().into(), val)
                    .unwrap();
            } else {
                ch.insert(address.as_bytes().to_vec().into(), val).unwrap();
            }
        }

        let mut ch2 = (new)();

        for (k, v) in (enc)(1, ch.clone()) {
            let (_, k, v) = (dec)(k, v);

            ch2.insert(k, v).unwrap();
        }

        assert_eq!(ch, ch2);
    }
}
