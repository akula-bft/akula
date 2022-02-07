use super::*;

macro_rules! interrupt {
    ( $(#[$outer:meta])* $name:ident => $resume_with:ty) => {
		$(#[$outer])*
        pub struct $name {
            pub(crate) inner: InnerCoroutine,
        }

        impl $name {
            pub fn resume(self, resume_data: $resume_with) -> Interrupt {
                resume_interrupt(self.inner, resume_data.into())
            }
        }
    };
}

interrupt! {
    /// EVM has just been created. Resume this interrupt to start execution.
    StartedInterrupt => ()
}
interrupt! {
    /// New instruction has been encountered.
    InstructionStartInterrupt => StateModifier
}
interrupt! {
    /// Does this account exist?
    AccountExistsInterrupt => AccountExistsStatus
}
interrupt! {
    /// Need this storage key.
    GetStorageInterrupt => StorageValue
}
interrupt! {
    /// Set this storage key.
    SetStorageInterrupt => StorageStatusInfo
}
interrupt! {
    /// Get balance of this account.
    GetBalanceInterrupt => Balance
}
interrupt! {
    /// Get code size of this account.
    GetCodeSizeInterrupt => CodeSize
}
interrupt! {
    /// Get code hash of this account.
    GetCodeHashInterrupt => CodeHash
}
interrupt! {
    /// Get code of this account.
    CopyCodeInterrupt  => Code
}
interrupt! {
    /// Selfdestruct this account.
    SelfdestructInterrupt  => ()
}
interrupt! {
    /// Execute this message as a new call.
    CallInterrupt => CallOutput
}
interrupt! {
    /// Get `TxContext` for this call.
    GetTxContextInterrupt => TxContextData
}
interrupt! {
    /// Get block hash for this account.
    GetBlockHashInterrupt => BlockHash
}
interrupt! {
    /// Emit log message.
    EmitLogInterrupt => ()
}
interrupt! {
    /// Access this account and return its status.
    AccessAccountInterrupt => AccessAccountStatus
}
interrupt! {
    /// Access this storage key and return its status.
    AccessStorageInterrupt => AccessStorageStatus
}

/// Execution complete, this interrupt cannot be resumed.
pub struct ExecutionComplete(pub(crate) InnerCoroutine);

/// Collection of all possible interrupts. Match on this to get the specific interrupt returned.
#[derive(From)]
pub enum Interrupt {
    InstructionStart {
        interrupt: InstructionStartInterrupt,
        pc: usize,
        opcode: OpCode,
        state: Box<ExecutionState>,
    },
    AccountExists {
        interrupt: AccountExistsInterrupt,
        address: Address,
    },
    GetStorage {
        interrupt: GetStorageInterrupt,
        address: Address,
        location: U256,
    },
    SetStorage {
        interrupt: SetStorageInterrupt,
        address: Address,
        location: U256,
        value: U256,
    },
    GetBalance {
        interrupt: GetBalanceInterrupt,
        address: Address,
    },
    GetCodeSize {
        interrupt: GetCodeSizeInterrupt,
        address: Address,
    },
    GetCodeHash {
        interrupt: GetCodeHashInterrupt,
        address: Address,
    },
    CopyCode {
        interrupt: CopyCodeInterrupt,
        address: Address,
        offset: usize,
        max_size: usize,
    },
    Selfdestruct {
        interrupt: SelfdestructInterrupt,
        address: Address,
        beneficiary: Address,
    },
    Call {
        interrupt: CallInterrupt,
        call_data: Call,
    },
    GetTxContext {
        interrupt: GetTxContextInterrupt,
    },
    GetBlockHash {
        interrupt: GetBlockHashInterrupt,
        block_number: u64,
    },
    EmitLog {
        interrupt: EmitLogInterrupt,
        address: Address,
        data: Bytes,
        topics: ArrayVec<U256, 4>,
    },
    AccessAccount {
        interrupt: AccessAccountInterrupt,
        address: Address,
    },
    AccessStorage {
        interrupt: AccessStorageInterrupt,
        address: Address,
        location: U256,
    },
    Complete {
        interrupt: ExecutionComplete,
        result: Result<SuccessfulOutput, StatusCode>,
    },
}
