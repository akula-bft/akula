use crate::{kv::*, models::*, Transaction};
use anyhow::Context;
use ethereum_types::H256;
use tracing::*;

pub async fn read_chain_config<'db: 'tx, 'tx, Tx: Transaction<'db>>(
    tx: &'tx Tx,
    block: H256,
) -> anyhow::Result<Option<ChainConfig>> {
    let key = block.as_bytes().into();

    trace!(
        "Reading chain config for block {:?} from at key {}",
        block,
        hex::encode(&key)
    );

    if let Some(b) = tx.get(&tables::Config, key).await? {
        trace!("Read chain config data: {}", hex::encode(&b));

        return Ok(Some(serde_json::from_slice(&*b).context("invalid JSON")?));
    }

    Ok(None)
}
