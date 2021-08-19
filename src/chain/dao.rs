use crate::{IntraBlockState, State};
use ethereum_types::*;

pub async fn transfer_balances<R>(
    state: &mut IntraBlockState<'_, R>,
    beneficiary: Address,
    drain: impl Iterator<Item = Address>,
) -> anyhow::Result<()>
where
    R: State,
{
    for address in drain {
        let b = state.get_balance(address).await?;
        state.add_to_balance(beneficiary, b).await?;
        state.set_balance(address, 0).await?;
    }

    Ok(())
}
