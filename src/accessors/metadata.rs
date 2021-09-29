use crate::{kv::*, models::*, Transaction};
use ethereum_types::H256;

pub async fn read_chain_config<'db, Tx: Transaction<'db>>(
    tx: &Tx,
    block: H256,
) -> anyhow::Result<Option<ChainConfig>> {
    tx.get(&tables::Config, block).await
}
