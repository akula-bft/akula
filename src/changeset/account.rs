pub use super::*;

impl ChangeSet {
    pub fn new_account() -> Self {
        Self {
            changes: vec![],
            key_len: H256::len_bytes(),
        }
    }
}

pub fn encode_accounts(
    block_n: u64,
    s: &ChangeSet,
    f: Box<dyn Fn(Bytes, Bytes) -> anyhow::Result<()> + Send + Sync>,
) -> anyhow::Result<()> {
    todo!()
}
