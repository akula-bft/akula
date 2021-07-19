use super::protocol_param::fee;
use crate::models::*;

pub fn intrinsic_gas(txn: &TransactionWithSender, homestead: bool, istanbul: bool) -> u128 {
    let mut gas = fee::G_TRANSACTION as u128;

    if matches!(txn.action, TransactionAction::Create) && homestead {
        gas += u128::from(fee::G_TX_CREATE);
    }

    // https://eips.ethereum.org/EIPS/eip-2930
    gas += txn.access_list.len() as u128 * u128::from(fee::ACCESS_LIST_ADDRESS_COST);
    for e in &txn.access_list {
        gas += e.slots.len() as u128 * u128::from(fee::ACCESS_LIST_STORAGE_KEY_COST);
    }

    if txn.input.is_empty() {
        return gas;
    }

    let non_zero_bytes = txn.input.iter().filter(|&&c| c != 0).count() as u128;

    let non_zero_gas = u128::from(if istanbul {
        fee::G_TX_DATA_NON_ZERO_ISTANBUL
    } else {
        fee::G_TX_DATA_NON_ZERO_FRONTIER
    });
    gas += non_zero_bytes * non_zero_gas;

    let zero_bytes = txn.input.len() as u128 - non_zero_bytes;
    gas += zero_bytes * u128::from(fee::G_TX_DATA_ZERO);

    gas
}
