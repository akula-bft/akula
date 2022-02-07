use super::*;
use educe::Educe;
use std::sync::Arc;

pub type StateModifier = Option<Arc<dyn Fn(&mut ExecutionState) + Send + Sync>>;

#[derive(Debug)]
pub struct AccountExistsStatus {
    pub exists: bool,
}

#[derive(Debug)]
pub struct Balance {
    pub balance: U256,
}

#[derive(Debug)]
pub struct CodeSize {
    pub code_size: U256,
}

#[derive(Debug)]
pub struct StorageValue {
    pub value: U256,
}

#[derive(Debug)]
pub struct StorageStatusInfo {
    pub status: StorageStatus,
}

#[derive(Debug)]
pub struct CodeHash {
    pub hash: U256,
}

#[derive(Debug)]
pub struct BlockHash {
    pub hash: U256,
}

#[derive(Debug)]
pub struct TxContextData {
    pub context: TxContext,
}

#[derive(Debug)]
pub struct Code {
    pub code: Bytes,
}

#[derive(Debug)]
pub struct CallOutput {
    pub output: Output,
}

#[derive(Debug)]
pub struct AccessAccountStatus {
    pub status: AccessStatus,
}

#[derive(Debug)]
pub struct AccessStorageStatus {
    pub status: AccessStatus,
}

/// All resumed data variants.
#[derive(Educe, EnumAsInner, From)]
#[educe(Debug)]
pub(crate) enum ResumeData {
    #[from(ignore)]
    Empty,
    StateModifier(#[educe(Debug(false))] StateModifier),
    AccountExistsStatus(AccountExistsStatus),
    Balance(Balance),
    CodeSize(CodeSize),
    StorageValue(StorageValue),
    StorageStatusInfo(StorageStatusInfo),
    CodeHash(CodeHash),
    BlockHash(BlockHash),
    TxContextData(TxContextData),
    Code(Code),
    CallOutput(CallOutput),
    AccessAccountStatus(AccessAccountStatus),
    AccessStorageStatus(AccessStorageStatus),
    Done(Infallible),
}

impl From<()> for ResumeData {
    fn from(_: ()) -> Self {
        Self::Empty
    }
}
