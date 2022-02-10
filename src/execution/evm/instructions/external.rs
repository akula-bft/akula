use crate::execution::evm::{common::address_to_u256, host::*, state::ExecutionState};
use ethnum::U256;

pub(crate) fn address(state: &mut ExecutionState) {
    state.stack.push(address_to_u256(state.message.recipient));
}

pub(crate) fn caller(state: &mut ExecutionState) {
    state.stack.push(address_to_u256(state.message.sender));
}

pub(crate) fn callvalue(state: &mut ExecutionState) {
    state.stack.push(state.message.value);
}

#[doc(hidden)]
#[macro_export]
macro_rules! balance {
    ($state:expr,$rev:expr) => {
        use $crate::{
            execution::evm::{
                common::*,
                continuation::{interrupt_data::*, resume_data::*},
                host::*,
                instructions::properties::*,
            },
            models::*,
        };

        let address = u256_to_address($state.stack.pop());

        if $rev >= Revision::Berlin {
            let access_status = ResumeData::into_access_account_status({
                yield InterruptData::AccessAccount { address }
            })
            .unwrap()
            .status;
            if access_status == AccessStatus::Cold {
                $state.gas_left -= i64::from(ADDITIONAL_COLD_ACCOUNT_ACCESS_COST);
                if $state.gas_left < 0 {
                    return Err(StatusCode::OutOfGas);
                }
            }
        }

        let balance = ResumeData::into_balance({ yield InterruptData::GetBalance { address } })
            .unwrap()
            .balance;

        $state.stack.push(balance);
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! extcodesize {
    ($state:expr,$rev:expr) => {
        use $crate::{
            execution::evm::{
                common::*,
                continuation::{interrupt_data::*, resume_data::*},
                host::*,
                instructions::properties::*,
            },
            models::*,
        };

        let address = u256_to_address($state.stack.pop());

        if $rev >= Revision::Berlin {
            let access_account = ResumeData::into_access_account_status({
                yield InterruptData::AccessAccount { address }
            })
            .unwrap()
            .status;
            if access_account == AccessStatus::Cold {
                $state.gas_left -= i64::from(ADDITIONAL_COLD_ACCOUNT_ACCESS_COST);
                if $state.gas_left < 0 {
                    return Err(StatusCode::OutOfGas);
                }
            }
        }

        let code_size =
            ResumeData::into_code_size({ yield InterruptData::GetCodeSize { address } })
                .unwrap()
                .code_size;
        $state.stack.push(code_size);
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! push_txcontext {
    ($state:expr, $accessor:expr) => {
        use $crate::execution::evm::continuation::{interrupt_data::*, resume_data::*};

        let tx_context = ResumeData::into_tx_context_data({ yield InterruptData::GetTxContext })
            .unwrap()
            .context;

        $state.stack.push($accessor(tx_context));
    };
}

pub(crate) fn origin_accessor(tx_context: TxContext) -> U256 {
    address_to_u256(tx_context.tx_origin)
}

pub(crate) fn coinbase_accessor(tx_context: TxContext) -> U256 {
    address_to_u256(tx_context.block_coinbase)
}

pub(crate) fn gasprice_accessor(tx_context: TxContext) -> U256 {
    tx_context.tx_gas_price
}

pub(crate) fn timestamp_accessor(tx_context: TxContext) -> U256 {
    tx_context.block_timestamp.into()
}

pub(crate) fn number_accessor(tx_context: TxContext) -> U256 {
    tx_context.block_number.into()
}

pub(crate) fn gaslimit_accessor(tx_context: TxContext) -> U256 {
    tx_context.block_gas_limit.into()
}

pub(crate) fn difficulty_accessor(tx_context: TxContext) -> U256 {
    tx_context.block_difficulty
}

pub(crate) fn chainid_accessor(tx_context: TxContext) -> U256 {
    tx_context.chain_id
}

pub(crate) fn basefee_accessor(tx_context: TxContext) -> U256 {
    tx_context.block_base_fee
}

#[doc(hidden)]
#[macro_export]
macro_rules! selfbalance {
    ($state:expr) => {{
        use $crate::execution::evm::continuation::{interrupt_data::*, resume_data::*};

        let balance = ResumeData::into_balance({
            yield InterruptData::GetBalance {
                address: $state.message.recipient,
            }
        })
        .unwrap()
        .balance;

        $state.stack.push(balance);
    }};
}

#[doc(hidden)]
#[macro_export]
macro_rules! blockhash {
    ($state:expr) => {
        use $crate::execution::evm::continuation::{interrupt_data::*, resume_data::*};

        let number = $state.stack.pop();

        let upper_bound = ResumeData::into_tx_context_data({ yield InterruptData::GetTxContext })
            .unwrap()
            .context
            .block_number;
        let lower_bound = upper_bound.saturating_sub(256);

        let mut header = U256::ZERO;
        if number <= u128::from(u64::MAX) {
            let n = number.as_u64();
            if (lower_bound..upper_bound).contains(&n) {
                header = ResumeData::into_block_hash({
                    yield InterruptData::GetBlockHash { block_number: n }
                })
                .unwrap()
                .hash;
            }
        }

        $state.stack.push(header);
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! do_log {
    ($state:expr, $num_topics:expr) => {{
        use arrayvec::ArrayVec;
        use $crate::execution::evm::continuation::{interrupt_data::*, resume_data::*};

        if $state.message.is_static {
            return Err(StatusCode::StaticModeViolation);
        }

        let offset = $state.stack.pop();
        let size = $state.stack.pop();

        let region =
            memory::get_memory_region($state, offset, size).map_err(|_| StatusCode::OutOfGas)?;

        if let Some(region) = &region {
            let cost = region.size.get() as i64 * 8;
            $state.gas_left -= cost;
            if $state.gas_left < 0 {
                return Err(StatusCode::OutOfGas);
            }
        }

        let mut topics = ArrayVec::new();
        for _ in 0..$num_topics {
            topics.push($state.stack.pop());
        }

        let data = if let Some(region) = region {
            &$state.memory[region.offset..region.offset + region.size.get()]
        } else {
            &[]
        };

        let data = data.to_vec().into();
        let r = {
            yield InterruptData::EmitLog {
                address: $state.message.recipient,
                data,
                topics,
            }
        };

        debug_assert!(matches!(r, ResumeData::Empty));
    }};
}

#[doc(hidden)]
#[macro_export]
macro_rules! sload {
    ($state:expr,$rev:expr) => {{
        use $crate::{
            execution::evm::{
                continuation::{interrupt_data::*, resume_data::*},
                host::*,
                instructions::properties::{COLD_SLOAD_COST, WARM_STORAGE_READ_COST},
            },
            models::*,
        };

        let location = $state.stack.pop();

        if $rev >= Revision::Berlin {
            let access_status = ResumeData::into_access_storage_status({
                yield InterruptData::AccessStorage {
                    address: $state.message.recipient,
                    location,
                }
            })
            .unwrap()
            .status;
            if access_status == AccessStatus::Cold {
                // The warm storage access cost is already applied (from the cost table).
                // Here we need to apply additional cold storage access cost.
                const ADDITIONAL_COLD_SLOAD_COST: u16 = COLD_SLOAD_COST - WARM_STORAGE_READ_COST;
                $state.gas_left -= i64::from(ADDITIONAL_COLD_SLOAD_COST);
                if $state.gas_left < 0 {
                    return Err(StatusCode::OutOfGas);
                }
            }
        }

        let storage = ResumeData::into_storage_value({
            yield InterruptData::GetStorage {
                address: $state.message.recipient,
                location,
            }
        })
        .unwrap()
        .value;

        $state.stack.push(storage);
    }};
}

#[doc(hidden)]
#[macro_export]
macro_rules! sstore {
    ($state:expr,$rev:expr) => {{
        use $crate::{
            execution::evm::{
                continuation::{interrupt_data::*, resume_data::*},
                host::*,
                instructions::properties::{COLD_SLOAD_COST, WARM_STORAGE_READ_COST},
            },
            models::*,
        };

        if $state.message.is_static {
            return Err(StatusCode::StaticModeViolation);
        }

        if $rev >= Revision::Istanbul {
            if $state.gas_left <= 2300 {
                return Err(StatusCode::OutOfGas);
            }
        }

        let location = $state.stack.pop();
        let value = $state.stack.pop();

        let mut cost = 0;
        if $rev >= Revision::Berlin {
            let access_status = ResumeData::into_access_storage_status({
                yield InterruptData::AccessStorage {
                    address: $state.message.recipient,
                    location,
                }
            })
            .unwrap()
            .status;

            if access_status == AccessStatus::Cold {
                cost = COLD_SLOAD_COST;
            }
        }

        let status = ResumeData::into_storage_status_info({
            yield InterruptData::SetStorage {
                address: $state.message.recipient,
                location,
                value,
            }
        })
        .unwrap()
        .status;

        cost = match status {
            StorageStatus::Unchanged | StorageStatus::ModifiedAgain => {
                if $rev >= Revision::Berlin {
                    cost + WARM_STORAGE_READ_COST
                } else if $rev == Revision::Istanbul {
                    800
                } else if $rev == Revision::Constantinople {
                    200
                } else {
                    5000
                }
            }
            StorageStatus::Modified | StorageStatus::Deleted => {
                if $rev >= Revision::Berlin {
                    cost + 5000 - COLD_SLOAD_COST
                } else {
                    5000
                }
            }
            StorageStatus::Added => cost + 20000,
        };
        $state.gas_left -= i64::from(cost);
        if $state.gas_left < 0 {
            return Err(StatusCode::OutOfGas);
        }
    }};
}

#[doc(hidden)]
#[macro_export]
macro_rules! selfdestruct {
    ($state:expr,$rev:expr) => {{
        use $crate::{
            execution::evm::{
                common::*,
                continuation::{interrupt_data::*, resume_data::*},
                host::*,
                instructions::properties::*,
            },
            models::*,
        };

        if $state.message.is_static {
            return Err(StatusCode::StaticModeViolation);
        }

        let beneficiary = u256_to_address($state.stack.pop());

        if $rev >= Revision::Berlin {
            let access_status = ResumeData::into_access_account_status({
                yield InterruptData::AccessAccount {
                    address: beneficiary,
                }
            })
            .unwrap()
            .status;
            if access_status == AccessStatus::Cold {
                $state.gas_left -= i64::from(COLD_ACCOUNT_ACCESS_COST);
                if $state.gas_left < 0 {
                    return Err(StatusCode::OutOfGas);
                }
            }
        }

        if $rev >= Revision::Tangerine {
            if ($rev == Revision::Tangerine
                || !{
                    ResumeData::into_balance({
                        yield InterruptData::GetBalance {
                            address: $state.message.recipient,
                        }
                    })
                    .unwrap()
                    .balance
                        == 0
                })
            {
                // After TANGERINE_WHISTLE apply additional cost of
                // sending value to a non-existing account.
                if !ResumeData::into_account_exists_status({
                    yield InterruptData::AccountExists {
                        address: beneficiary,
                    }
                })
                .unwrap()
                .exists
                {
                    $state.gas_left -= 25000;
                    if $state.gas_left < 0 {
                        return Err(StatusCode::OutOfGas);
                    }
                }
            }
        }

        let r = yield InterruptData::Selfdestruct {
            address: $state.message.recipient,
            beneficiary,
        };
        debug_assert!(matches!(r, ResumeData::Empty));
    }};
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
