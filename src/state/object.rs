use crate::models::*;
use std::collections::HashMap;

#[derive(Clone, Debug, Default)]
pub struct Object {
    pub initial: Option<Account>,
    pub current: Option<Account>,
}

#[derive(Debug, Default)]
pub struct CommittedValue {
    /// Value at the begining of the block
    pub initial: U256,
    /// Value at the begining of the transaction; see EIP-2200
    pub original: U256,
}

#[derive(Debug, Default)]
pub struct Storage {
    pub committed: HashMap<U256, CommittedValue>,
    pub current: HashMap<U256, U256>,
}
