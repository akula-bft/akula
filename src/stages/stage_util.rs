use crate::{
    kv::{
        tables,
        traits::{Transaction, *},
    },
    models::*,
};
use anyhow::format_err;

pub async fn should_do_clean_promotion<'db, 'tx, Tx>(
    tx: &'tx Tx,
    genesis: BlockNumber,
    past_progress: BlockNumber,
    max_block: BlockNumber,
    threshold: u64,
) -> anyhow::Result<bool>
where
    'db: 'tx,
    Tx: Transaction<'db>,
{
    // TODO: remove me when proper unwind is implemented
    if tx
        .cursor(&tables::TrieAccount)
        .await?
        .first()
        .await?
        .is_none()
    {
        return Ok(true);
    }

    let current_gas = tx
        .get(&tables::CumulativeIndex, past_progress)
        .await?
        .ok_or_else(|| format_err!("No cumulative index for block {}", past_progress))?
        .gas;
    let max_gas = tx
        .get(&tables::CumulativeIndex, max_block)
        .await?
        .ok_or_else(|| format_err!("No cumulative index for block {}", max_block))?
        .gas;

    let gas_progress = max_gas.checked_sub(current_gas).ok_or_else(|| {
        format_err!(
            "Faulty cumulative index: max gas less than current gas ({} < {})",
            max_gas,
            current_gas
        )
    })?;

    Ok(past_progress == genesis || gas_progress > threshold)
}
