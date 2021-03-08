use crate::{buckets, common, dbutils::*, models::*, txutil, Transaction};
use anyhow::Context;
use ethereum_types::H256;
use tracing::*;

pub async fn read_chain_config<'tx, Tx: Transaction<'tx>>(
    tx: &'tx Tx,
    block: H256,
) -> anyhow::Result<Option<ChainConfig>> {
    let key = block.as_bytes();

    trace!(
        "Reading chain config for block {:?} from at key {}",
        block,
        hex::encode(&key)
    );

    let b = txutil::get_one::<_, buckets::Config>(tx, &key).await?;

    trace!("Read chain config data: {}", hex::encode(&b));

    if b.is_empty() {
        return Ok(None);
    }

    Ok(Some(serde_json::from_slice(&*b).context("invalid JSON")?))
}
