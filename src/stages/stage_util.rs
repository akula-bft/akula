use crate::{
    kv::{mdbx::*, tables},
    models::*,
};
use anyhow::format_err;

pub fn should_do_clean_promotion<'db, 'tx, K, E>(
    tx: &'tx MdbxTransaction<'db, K, E>,
    genesis: BlockNumber,
    past_progress: BlockNumber,
    max_block: BlockNumber,
    threshold: u64,
) -> anyhow::Result<bool>
where
    'db: 'tx,
    K: TransactionKind,
    E: EnvironmentKind,
{
    let current_gas = tx
        .get(tables::TotalGas, past_progress)?
        .ok_or_else(|| format_err!("No cumulative index for block {}", past_progress))?;
    let max_gas = tx
        .get(tables::TotalGas, max_block)?
        .ok_or_else(|| format_err!("No cumulative index for block {}", max_block))?;

    let gas_progress = max_gas.checked_sub(current_gas).ok_or_else(|| {
        format_err!(
            "Faulty cumulative index: max gas less than current gas ({} < {})",
            max_gas,
            current_gas
        )
    })?;

    Ok(past_progress == genesis || gas_progress > threshold)
}
