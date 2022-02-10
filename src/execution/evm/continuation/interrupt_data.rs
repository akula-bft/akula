use super::*;

#[derive(Debug)]
pub enum Call {
    Call(InterpreterMessage),
    Create(CreateMessage),
}

#[derive(Debug)]
pub enum InterruptData {
    InstructionStart {
        pc: usize,
        opcode: OpCode,
        state: Box<ExecutionState>,
    },
    AccountExists {
        address: Address,
    },
    GetStorage {
        address: Address,
        location: U256,
    },
    SetStorage {
        address: Address,
        location: U256,
        value: U256,
    },
    GetBalance {
        address: Address,
    },
    GetCodeSize {
        address: Address,
    },
    GetCodeHash {
        address: Address,
    },
    CopyCode {
        address: Address,
        offset: usize,
        max_size: usize,
    },
    Selfdestruct {
        address: Address,
        beneficiary: Address,
    },
    Call(Call),
    GetTxContext,
    GetBlockHash {
        block_number: u64,
    },
    EmitLog {
        address: Address,
        data: Bytes,
        topics: ArrayVec<U256, 4>,
    },
    AccessAccount {
        address: Address,
    },
    AccessStorage {
        address: Address,
        location: U256,
    },
}
