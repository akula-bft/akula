use crate::{
    execution::evm::{
        common::address_to_u256, host::*, instructions::memory, state::ExecutionState, StatusCode,
    },
    models::Revision,
};
use ethnum::U256;

#[inline(always)]
fn ok_or_out_of_gas(gas_left: i64) -> Result<(), StatusCode> {
    let enough_gas = gas_left >= 0;
    [Err(StatusCode::OutOfGas), Ok(())][enough_gas as usize]
}

#[inline]
pub(crate) fn address(state: &mut ExecutionState) {
    let res = address_to_u256(state.message.recipient);
    state.stack.push(res);
}

#[inline]
pub(crate) fn caller(state: &mut ExecutionState) {
    let res = address_to_u256(state.message.sender);
    state.stack.push(res);
}

#[inline]
pub(crate) fn callvalue(state: &mut ExecutionState) {
    let res = state.message.value;
    state.stack.push(res);
}

#[inline]
#[allow(clippy::collapsible_if)]
pub(crate) fn balance<H: Host, const REVISION: Revision>(
    state: &mut ExecutionState,
    host: &mut H,
) -> Result<(), StatusCode> {
    use crate::{
        execution::evm::{common::*, host::*, instructions::properties::*},
        models::*,
    };

    let address = u256_to_address(state.stack.pop());

    let is_cold =
        REVISION >= Revision::Berlin && host.access_account(address) == AccessStatus::Cold;
    let additional_cost = is_cold as i64 * ADDITIONAL_COLD_ACCOUNT_ACCESS_COST as i64;
    state.gas_left -= additional_cost;

    state.stack.push(host.get_balance(address));

    ok_or_out_of_gas(state.gas_left)
}

#[inline]
#[allow(clippy::collapsible_if)]
pub(crate) fn extcodesize<H: Host, const REVISION: Revision>(
    state: &mut ExecutionState,
    host: &mut H,
) -> Result<(), StatusCode> {
    use crate::{
        execution::evm::{common::*, host::*, instructions::properties::*},
        models::*,
    };

    let address = u256_to_address(state.stack.pop());

    let is_cold =
        REVISION >= Revision::Berlin && host.access_account(address) == AccessStatus::Cold;
    let additional_cost = is_cold as i64 * ADDITIONAL_COLD_ACCOUNT_ACCESS_COST as i64;

    state.stack.push(host.get_code_size(address));
    state.gas_left -= additional_cost;

    ok_or_out_of_gas(state.gas_left)
}

#[inline]
pub(crate) fn origin_accessor(tx_context: TxContext) -> U256 {
    address_to_u256(tx_context.tx_origin)
}

#[inline]
pub(crate) fn coinbase_accessor(tx_context: TxContext) -> U256 {
    address_to_u256(tx_context.block_coinbase)
}

#[inline]
pub(crate) fn gasprice_accessor(tx_context: TxContext) -> U256 {
    tx_context.tx_gas_price
}

#[inline]
pub(crate) fn timestamp_accessor(tx_context: TxContext) -> U256 {
    tx_context.block_timestamp.into()
}

#[inline]
pub(crate) fn number_accessor(tx_context: TxContext) -> U256 {
    tx_context.block_number.into()
}

#[inline]
pub(crate) fn gaslimit_accessor(tx_context: TxContext) -> U256 {
    tx_context.block_gas_limit.into()
}

#[inline]
pub(crate) fn difficulty_accessor(tx_context: TxContext) -> U256 {
    tx_context.block_difficulty
}

#[inline]
pub(crate) fn chainid_accessor(tx_context: TxContext) -> U256 {
    tx_context.chain_id
}

#[inline]
pub(crate) fn basefee_accessor(tx_context: TxContext) -> U256 {
    tx_context.block_base_fee
}

#[inline]
pub(crate) fn selfbalance<H: Host>(state: &mut ExecutionState, host: &mut H) {
    state.stack.push(host.get_balance(state.message.recipient));
}

#[inline]
pub(crate) fn blockhash<H: Host>(
    state: &mut ExecutionState,
    host: &mut H,
) -> Result<(), StatusCode> {
    let number = state.stack.pop();

    let upper_bound = host.get_tx_context()?.block_number;
    let lower_bound = upper_bound.saturating_sub(256);

    let mut header = U256::ZERO;
    if number <= u128::from(u64::MAX) {
        let n = number.as_u64();
        if (lower_bound..upper_bound).contains(&n) {
            header = host.get_block_hash(n);
        }
    }

    state.stack.push(header);

    Ok(())
}

#[inline]
#[allow(clippy::collapsible_if)]
pub(crate) fn do_log<H: Host, const NUM_TOPICS: usize>(
    state: &mut ExecutionState,
    host: &mut H,
) -> Result<(), StatusCode> {
    if state.message.is_static {
        return Err(StatusCode::StaticModeViolation);
    }

    let offset = state.stack.pop();
    let size = state.stack.pop();

    let region = memory::get_memory_region(state, offset, size)?;

    if let Some(region) = &region {
        let cost = region.size.get() as i64 * 8;
        state.gas_left -= cost;
        if state.gas_left < 0 {
            return Err(StatusCode::OutOfGas);
        }
    }

    let topics = [(); NUM_TOPICS].map(|()| state.stack.pop());
    let data = if let Some(region) = region {
        &state.memory[region.offset..][..region.size.get()]
    } else {
        &[]
    };

    host.emit_log(state.message.recipient, data.to_vec().into(), &topics);

    Ok(())
}

#[inline]
#[allow(clippy::collapsible_if)]
pub(crate) fn sload<H: Host, const REVISION: Revision>(
    state: &mut ExecutionState,
    host: &mut H,
) -> Result<(), StatusCode> {
    use crate::{
        execution::evm::{
            host::*,
            instructions::properties::{COLD_SLOAD_COST, WARM_STORAGE_READ_COST},
        },
        models::*,
    };

    let location = state.stack.pop();

    const ADDITIONAL_COLD_SLOAD_COST: u16 = COLD_SLOAD_COST - WARM_STORAGE_READ_COST;
    let is_cold = REVISION >= Revision::Berlin
        && host.access_storage(state.message.recipient, location) == AccessStatus::Cold;
    let additional_cost = is_cold as i64 * ADDITIONAL_COLD_SLOAD_COST as i64;

    state
        .stack
        .push(host.get_storage(state.message.recipient, location));

    state.gas_left -= additional_cost;
    ok_or_out_of_gas(state.gas_left)
}

#[inline]
#[allow(clippy::collapsible_if)]
pub(crate) fn sstore<H: Host, const REVISION: Revision>(
    state: &mut ExecutionState,
    host: &mut H,
) -> Result<(), StatusCode> {
    use crate::{
        execution::evm::{
            host::*,
            instructions::properties::{COLD_SLOAD_COST, WARM_STORAGE_READ_COST},
        },
        models::*,
    };

    if state.message.is_static {
        return Err(StatusCode::StaticModeViolation);
    }

    if REVISION >= Revision::Istanbul {
        if state.gas_left <= 2300 {
            return Err(StatusCode::OutOfGas);
        }
    }

    let location = state.stack.pop();
    let value = state.stack.pop();

    let mut cost = 0;
    if REVISION >= Revision::Berlin {
        if host.access_storage(state.message.recipient, location) == AccessStatus::Cold {
            cost = COLD_SLOAD_COST;
        }
    }

    cost = match host.set_storage(state.message.recipient, location, value) {
        StorageStatus::Unchanged | StorageStatus::ModifiedAgain => {
            if REVISION >= Revision::Berlin {
                cost + WARM_STORAGE_READ_COST
            } else if REVISION == Revision::Istanbul {
                800
            } else if REVISION == Revision::Constantinople {
                200
            } else {
                5000
            }
        }
        StorageStatus::Modified | StorageStatus::Deleted => {
            if REVISION >= Revision::Berlin {
                cost + 5000 - COLD_SLOAD_COST
            } else {
                5000
            }
        }
        StorageStatus::Added => cost + 20000,
    };
    state.gas_left -= i64::from(cost);

    ok_or_out_of_gas(state.gas_left)
}

#[inline]
#[allow(clippy::collapsible_if)]
pub(crate) fn selfdestruct<H: Host, const REVISION: Revision>(
    state: &mut ExecutionState,
    host: &mut H,
) -> Result<(), StatusCode> {
    use crate::{
        execution::evm::{common::*, host::*, instructions::properties::*},
        models::*,
    };

    if state.message.is_static {
        return Err(StatusCode::StaticModeViolation);
    }

    let beneficiary = u256_to_address(state.stack.pop());

    let is_cold =
        REVISION >= Revision::Berlin && host.access_account(beneficiary) == AccessStatus::Cold;
    let has_nonexisting_account_fee = REVISION == Revision::Tangerine
        || (REVISION > Revision::Tangerine && host.get_balance(state.message.recipient) != 0);
    let is_punished = has_nonexisting_account_fee && !host.account_exists(beneficiary);

    let additional_cost =
        is_cold as i64 * COLD_ACCOUNT_ACCESS_COST as i64 + is_punished as i64 * 25000i64;

    state.gas_left -= additional_cost;
    if state.gas_left < 0 {
        return Err(StatusCode::OutOfGas);
    }

    host.selfdestruct(state.message.recipient, beneficiary);

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::execution::evm::common::u256_to_address;
    use ethereum_types::Address;
    use hex_literal::hex;

    #[test]
    fn u256_to_address_conversion() {
        assert_eq!(
            u256_to_address(0x42_u128.into()),
            Address::from(hex!("0000000000000000000000000000000000000042"))
        );
    }
}
