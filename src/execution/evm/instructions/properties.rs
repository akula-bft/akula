use crate::{execution::evm::opcode::*, models::*};

pub(crate) const COLD_SLOAD_COST: u16 = 2100;
pub(crate) const COLD_ACCOUNT_ACCESS_COST: u16 = 2600;
pub(crate) const WARM_STORAGE_READ_COST: u16 = 100;

/// Additional cold account access cost.
///
/// The warm access cost is unconditionally applied for every account access instruction.
/// If the access turns out to be cold, this cost must be applied additionally.
pub(crate) const ADDITIONAL_COLD_ACCOUNT_ACCESS_COST: u16 =
    COLD_ACCOUNT_ACCESS_COST - WARM_STORAGE_READ_COST;

/// EVM instruction properties
#[derive(Clone, Copy, Debug)]
pub struct Properties {
    /// The number of stack items the instruction accesses during execution.
    pub stack_height_required: u8,
    /// The stack height change caused by the instruction execution. Can be negative.
    pub stack_height_change: i8,
}

impl Properties {
    const fn new(stack_height_required: u8, stack_height_change: i8) -> Self {
        Self {
            stack_height_required,
            stack_height_change,
        }
    }
}

pub const fn properties<const OPCODE: OpCode>() -> Properties {
    match OPCODE {
        OpCode::STOP => Properties::new(0, 0),
        OpCode::ADD => Properties::new(2, -1),
        OpCode::MUL => Properties::new(2, -1),
        OpCode::SUB => Properties::new(2, -1),
        OpCode::DIV => Properties::new(2, -1),
        OpCode::SDIV => Properties::new(2, -1),
        OpCode::MOD => Properties::new(2, -1),
        OpCode::SMOD => Properties::new(2, -1),
        OpCode::ADDMOD => Properties::new(3, -2),
        OpCode::MULMOD => Properties::new(3, -2),
        OpCode::EXP => Properties::new(2, -1),
        OpCode::SIGNEXTEND => Properties::new(2, -1),

        OpCode::LT => Properties::new(2, -1),
        OpCode::GT => Properties::new(2, -1),
        OpCode::SLT => Properties::new(2, -1),
        OpCode::SGT => Properties::new(2, -1),
        OpCode::EQ => Properties::new(2, -1),
        OpCode::ISZERO => Properties::new(1, 0),
        OpCode::AND => Properties::new(2, -1),
        OpCode::OR => Properties::new(2, -1),
        OpCode::XOR => Properties::new(2, -1),
        OpCode::NOT => Properties::new(1, 0),
        OpCode::BYTE => Properties::new(2, -1),
        OpCode::SHL => Properties::new(2, -1),
        OpCode::SHR => Properties::new(2, -1),
        OpCode::SAR => Properties::new(2, -1),

        OpCode::KECCAK256 => Properties::new(2, -1),

        OpCode::ADDRESS => Properties::new(0, 1),
        OpCode::BALANCE => Properties::new(1, 0),
        OpCode::ORIGIN => Properties::new(0, 1),
        OpCode::CALLER => Properties::new(0, 1),
        OpCode::CALLVALUE => Properties::new(0, 1),
        OpCode::CALLDATALOAD => Properties::new(1, 0),
        OpCode::CALLDATASIZE => Properties::new(0, 1),
        OpCode::CALLDATACOPY => Properties::new(3, -3),
        OpCode::CODESIZE => Properties::new(0, 1),
        OpCode::CODECOPY => Properties::new(3, -3),
        OpCode::GASPRICE => Properties::new(0, 1),
        OpCode::EXTCODESIZE => Properties::new(1, 0),
        OpCode::EXTCODECOPY => Properties::new(4, -4),
        OpCode::RETURNDATASIZE => Properties::new(0, 1),
        OpCode::RETURNDATACOPY => Properties::new(3, -3),
        OpCode::EXTCODEHASH => Properties::new(1, 0),

        OpCode::BLOCKHASH => Properties::new(1, 0),
        OpCode::COINBASE => Properties::new(0, 1),
        OpCode::TIMESTAMP => Properties::new(0, 1),
        OpCode::NUMBER => Properties::new(0, 1),
        OpCode::DIFFICULTY => Properties::new(0, 1),
        OpCode::GASLIMIT => Properties::new(0, 1),
        OpCode::CHAINID => Properties::new(0, 1),
        OpCode::SELFBALANCE => Properties::new(0, 1),
        OpCode::BASEFEE => Properties::new(0, 1),

        OpCode::POP => Properties::new(1, -1),
        OpCode::MLOAD => Properties::new(1, 0),
        OpCode::MSTORE => Properties::new(2, -2),
        OpCode::MSTORE8 => Properties::new(2, -2),
        OpCode::SLOAD => Properties::new(1, 0),
        OpCode::SSTORE => Properties::new(2, -2),
        OpCode::JUMP => Properties::new(1, -1),
        OpCode::JUMPI => Properties::new(2, -2),
        OpCode::PC => Properties::new(0, 1),
        OpCode::MSIZE => Properties::new(0, 1),
        OpCode::GAS => Properties::new(0, 1),
        OpCode::JUMPDEST => Properties::new(0, 0),

        OpCode::PUSH1 => Properties::new(0, 1),
        OpCode::PUSH2 => Properties::new(0, 1),
        OpCode::PUSH3 => Properties::new(0, 1),
        OpCode::PUSH4 => Properties::new(0, 1),
        OpCode::PUSH5 => Properties::new(0, 1),
        OpCode::PUSH6 => Properties::new(0, 1),
        OpCode::PUSH7 => Properties::new(0, 1),
        OpCode::PUSH8 => Properties::new(0, 1),
        OpCode::PUSH9 => Properties::new(0, 1),
        OpCode::PUSH10 => Properties::new(0, 1),
        OpCode::PUSH11 => Properties::new(0, 1),
        OpCode::PUSH12 => Properties::new(0, 1),
        OpCode::PUSH13 => Properties::new(0, 1),
        OpCode::PUSH14 => Properties::new(0, 1),
        OpCode::PUSH15 => Properties::new(0, 1),
        OpCode::PUSH16 => Properties::new(0, 1),
        OpCode::PUSH17 => Properties::new(0, 1),
        OpCode::PUSH18 => Properties::new(0, 1),
        OpCode::PUSH19 => Properties::new(0, 1),
        OpCode::PUSH20 => Properties::new(0, 1),
        OpCode::PUSH21 => Properties::new(0, 1),
        OpCode::PUSH22 => Properties::new(0, 1),
        OpCode::PUSH23 => Properties::new(0, 1),
        OpCode::PUSH24 => Properties::new(0, 1),
        OpCode::PUSH25 => Properties::new(0, 1),
        OpCode::PUSH26 => Properties::new(0, 1),
        OpCode::PUSH27 => Properties::new(0, 1),
        OpCode::PUSH28 => Properties::new(0, 1),
        OpCode::PUSH29 => Properties::new(0, 1),
        OpCode::PUSH30 => Properties::new(0, 1),
        OpCode::PUSH31 => Properties::new(0, 1),
        OpCode::PUSH32 => Properties::new(0, 1),

        OpCode::DUP1 => Properties::new(1, 1),
        OpCode::DUP2 => Properties::new(2, 1),
        OpCode::DUP3 => Properties::new(3, 1),
        OpCode::DUP4 => Properties::new(4, 1),
        OpCode::DUP5 => Properties::new(5, 1),
        OpCode::DUP6 => Properties::new(6, 1),
        OpCode::DUP7 => Properties::new(7, 1),
        OpCode::DUP8 => Properties::new(8, 1),
        OpCode::DUP9 => Properties::new(9, 1),
        OpCode::DUP10 => Properties::new(10, 1),
        OpCode::DUP11 => Properties::new(11, 1),
        OpCode::DUP12 => Properties::new(12, 1),
        OpCode::DUP13 => Properties::new(13, 1),
        OpCode::DUP14 => Properties::new(14, 1),
        OpCode::DUP15 => Properties::new(15, 1),
        OpCode::DUP16 => Properties::new(16, 1),

        OpCode::SWAP1 => Properties::new(2, 0),
        OpCode::SWAP2 => Properties::new(3, 0),
        OpCode::SWAP3 => Properties::new(4, 0),
        OpCode::SWAP4 => Properties::new(5, 0),
        OpCode::SWAP5 => Properties::new(6, 0),
        OpCode::SWAP6 => Properties::new(7, 0),
        OpCode::SWAP7 => Properties::new(8, 0),
        OpCode::SWAP8 => Properties::new(9, 0),
        OpCode::SWAP9 => Properties::new(10, 0),
        OpCode::SWAP10 => Properties::new(11, 0),
        OpCode::SWAP11 => Properties::new(12, 0),
        OpCode::SWAP12 => Properties::new(13, 0),
        OpCode::SWAP13 => Properties::new(14, 0),
        OpCode::SWAP14 => Properties::new(15, 0),
        OpCode::SWAP15 => Properties::new(16, 0),
        OpCode::SWAP16 => Properties::new(17, 0),

        OpCode::LOG0 => Properties::new(2, -2),
        OpCode::LOG1 => Properties::new(3, -3),
        OpCode::LOG2 => Properties::new(4, -4),
        OpCode::LOG3 => Properties::new(5, -5),
        OpCode::LOG4 => Properties::new(6, -6),

        OpCode::CREATE => Properties::new(3, -2),
        OpCode::CALL => Properties::new(7, -6),
        OpCode::CALLCODE => Properties::new(7, -6),
        OpCode::RETURN => Properties::new(2, -2),
        OpCode::DELEGATECALL => Properties::new(6, -5),
        OpCode::CREATE2 => Properties::new(4, -3),
        OpCode::STATICCALL => Properties::new(6, -5),
        OpCode::REVERT => Properties::new(2, -2),
        OpCode::INVALID => Properties::new(0, 0),
        OpCode::SELFDESTRUCT => Properties::new(1, -1),
        _ => unreachable!(),
    }
}

pub type GasCostTable = [[i16; 256]; Revision::len()];

const fn gas_costs() -> GasCostTable {
    let mut table = [[-1; 256]; Revision::len()];

    table[Revision::Frontier as usize][OpCode::STOP.to_usize()] = 0;
    table[Revision::Frontier as usize][OpCode::ADD.to_usize()] = 3;
    table[Revision::Frontier as usize][OpCode::MUL.to_usize()] = 5;
    table[Revision::Frontier as usize][OpCode::SUB.to_usize()] = 3;
    table[Revision::Frontier as usize][OpCode::DIV.to_usize()] = 5;
    table[Revision::Frontier as usize][OpCode::SDIV.to_usize()] = 5;
    table[Revision::Frontier as usize][OpCode::MOD.to_usize()] = 5;
    table[Revision::Frontier as usize][OpCode::SMOD.to_usize()] = 5;
    table[Revision::Frontier as usize][OpCode::ADDMOD.to_usize()] = 8;
    table[Revision::Frontier as usize][OpCode::MULMOD.to_usize()] = 8;
    table[Revision::Frontier as usize][OpCode::EXP.to_usize()] = 10;
    table[Revision::Frontier as usize][OpCode::SIGNEXTEND.to_usize()] = 5;
    table[Revision::Frontier as usize][OpCode::LT.to_usize()] = 3;
    table[Revision::Frontier as usize][OpCode::GT.to_usize()] = 3;
    table[Revision::Frontier as usize][OpCode::SLT.to_usize()] = 3;
    table[Revision::Frontier as usize][OpCode::SGT.to_usize()] = 3;
    table[Revision::Frontier as usize][OpCode::EQ.to_usize()] = 3;
    table[Revision::Frontier as usize][OpCode::ISZERO.to_usize()] = 3;
    table[Revision::Frontier as usize][OpCode::AND.to_usize()] = 3;
    table[Revision::Frontier as usize][OpCode::OR.to_usize()] = 3;
    table[Revision::Frontier as usize][OpCode::XOR.to_usize()] = 3;
    table[Revision::Frontier as usize][OpCode::NOT.to_usize()] = 3;
    table[Revision::Frontier as usize][OpCode::BYTE.to_usize()] = 3;
    table[Revision::Frontier as usize][OpCode::KECCAK256.to_usize()] = 30;
    table[Revision::Frontier as usize][OpCode::ADDRESS.to_usize()] = 2;
    table[Revision::Frontier as usize][OpCode::BALANCE.to_usize()] = 20;
    table[Revision::Frontier as usize][OpCode::ORIGIN.to_usize()] = 2;
    table[Revision::Frontier as usize][OpCode::CALLER.to_usize()] = 2;
    table[Revision::Frontier as usize][OpCode::CALLVALUE.to_usize()] = 2;
    table[Revision::Frontier as usize][OpCode::CALLDATALOAD.to_usize()] = 3;
    table[Revision::Frontier as usize][OpCode::CALLDATASIZE.to_usize()] = 2;
    table[Revision::Frontier as usize][OpCode::CALLDATACOPY.to_usize()] = 3;
    table[Revision::Frontier as usize][OpCode::CODESIZE.to_usize()] = 2;
    table[Revision::Frontier as usize][OpCode::CODECOPY.to_usize()] = 3;
    table[Revision::Frontier as usize][OpCode::GASPRICE.to_usize()] = 2;
    table[Revision::Frontier as usize][OpCode::EXTCODESIZE.to_usize()] = 20;
    table[Revision::Frontier as usize][OpCode::EXTCODECOPY.to_usize()] = 20;
    table[Revision::Frontier as usize][OpCode::BLOCKHASH.to_usize()] = 20;
    table[Revision::Frontier as usize][OpCode::COINBASE.to_usize()] = 2;
    table[Revision::Frontier as usize][OpCode::TIMESTAMP.to_usize()] = 2;
    table[Revision::Frontier as usize][OpCode::NUMBER.to_usize()] = 2;
    table[Revision::Frontier as usize][OpCode::DIFFICULTY.to_usize()] = 2;
    table[Revision::Frontier as usize][OpCode::GASLIMIT.to_usize()] = 2;
    table[Revision::Frontier as usize][OpCode::POP.to_usize()] = 2;
    table[Revision::Frontier as usize][OpCode::MLOAD.to_usize()] = 3;
    table[Revision::Frontier as usize][OpCode::MSTORE.to_usize()] = 3;
    table[Revision::Frontier as usize][OpCode::MSTORE8.to_usize()] = 3;
    table[Revision::Frontier as usize][OpCode::SLOAD.to_usize()] = 50;
    table[Revision::Frontier as usize][OpCode::SSTORE.to_usize()] = 0;
    table[Revision::Frontier as usize][OpCode::JUMP.to_usize()] = 8;
    table[Revision::Frontier as usize][OpCode::JUMPI.to_usize()] = 10;
    table[Revision::Frontier as usize][OpCode::PC.to_usize()] = 2;
    table[Revision::Frontier as usize][OpCode::MSIZE.to_usize()] = 2;
    table[Revision::Frontier as usize][OpCode::GAS.to_usize()] = 2;
    table[Revision::Frontier as usize][OpCode::JUMPDEST.to_usize()] = 1;

    table[Revision::Frontier as usize][OpCode::PUSH1.to_usize()] = 3;
    table[Revision::Frontier as usize][OpCode::PUSH2.to_usize()] = 3;
    table[Revision::Frontier as usize][OpCode::PUSH3.to_usize()] = 3;
    table[Revision::Frontier as usize][OpCode::PUSH4.to_usize()] = 3;
    table[Revision::Frontier as usize][OpCode::PUSH5.to_usize()] = 3;
    table[Revision::Frontier as usize][OpCode::PUSH6.to_usize()] = 3;
    table[Revision::Frontier as usize][OpCode::PUSH7.to_usize()] = 3;
    table[Revision::Frontier as usize][OpCode::PUSH8.to_usize()] = 3;
    table[Revision::Frontier as usize][OpCode::PUSH9.to_usize()] = 3;
    table[Revision::Frontier as usize][OpCode::PUSH10.to_usize()] = 3;
    table[Revision::Frontier as usize][OpCode::PUSH11.to_usize()] = 3;
    table[Revision::Frontier as usize][OpCode::PUSH12.to_usize()] = 3;
    table[Revision::Frontier as usize][OpCode::PUSH13.to_usize()] = 3;
    table[Revision::Frontier as usize][OpCode::PUSH14.to_usize()] = 3;
    table[Revision::Frontier as usize][OpCode::PUSH15.to_usize()] = 3;
    table[Revision::Frontier as usize][OpCode::PUSH16.to_usize()] = 3;
    table[Revision::Frontier as usize][OpCode::PUSH17.to_usize()] = 3;
    table[Revision::Frontier as usize][OpCode::PUSH18.to_usize()] = 3;
    table[Revision::Frontier as usize][OpCode::PUSH19.to_usize()] = 3;
    table[Revision::Frontier as usize][OpCode::PUSH20.to_usize()] = 3;
    table[Revision::Frontier as usize][OpCode::PUSH21.to_usize()] = 3;
    table[Revision::Frontier as usize][OpCode::PUSH22.to_usize()] = 3;
    table[Revision::Frontier as usize][OpCode::PUSH23.to_usize()] = 3;
    table[Revision::Frontier as usize][OpCode::PUSH24.to_usize()] = 3;
    table[Revision::Frontier as usize][OpCode::PUSH25.to_usize()] = 3;
    table[Revision::Frontier as usize][OpCode::PUSH26.to_usize()] = 3;
    table[Revision::Frontier as usize][OpCode::PUSH27.to_usize()] = 3;
    table[Revision::Frontier as usize][OpCode::PUSH28.to_usize()] = 3;
    table[Revision::Frontier as usize][OpCode::PUSH29.to_usize()] = 3;
    table[Revision::Frontier as usize][OpCode::PUSH30.to_usize()] = 3;
    table[Revision::Frontier as usize][OpCode::PUSH31.to_usize()] = 3;
    table[Revision::Frontier as usize][OpCode::PUSH32.to_usize()] = 3;

    table[Revision::Frontier as usize][OpCode::DUP1.to_usize()] = 3;
    table[Revision::Frontier as usize][OpCode::DUP2.to_usize()] = 3;
    table[Revision::Frontier as usize][OpCode::DUP3.to_usize()] = 3;
    table[Revision::Frontier as usize][OpCode::DUP4.to_usize()] = 3;
    table[Revision::Frontier as usize][OpCode::DUP5.to_usize()] = 3;
    table[Revision::Frontier as usize][OpCode::DUP6.to_usize()] = 3;
    table[Revision::Frontier as usize][OpCode::DUP7.to_usize()] = 3;
    table[Revision::Frontier as usize][OpCode::DUP8.to_usize()] = 3;
    table[Revision::Frontier as usize][OpCode::DUP9.to_usize()] = 3;
    table[Revision::Frontier as usize][OpCode::DUP10.to_usize()] = 3;
    table[Revision::Frontier as usize][OpCode::DUP11.to_usize()] = 3;
    table[Revision::Frontier as usize][OpCode::DUP12.to_usize()] = 3;
    table[Revision::Frontier as usize][OpCode::DUP13.to_usize()] = 3;
    table[Revision::Frontier as usize][OpCode::DUP14.to_usize()] = 3;
    table[Revision::Frontier as usize][OpCode::DUP15.to_usize()] = 3;
    table[Revision::Frontier as usize][OpCode::DUP16.to_usize()] = 3;

    table[Revision::Frontier as usize][OpCode::SWAP1.to_usize()] = 3;
    table[Revision::Frontier as usize][OpCode::SWAP2.to_usize()] = 3;
    table[Revision::Frontier as usize][OpCode::SWAP3.to_usize()] = 3;
    table[Revision::Frontier as usize][OpCode::SWAP4.to_usize()] = 3;
    table[Revision::Frontier as usize][OpCode::SWAP5.to_usize()] = 3;
    table[Revision::Frontier as usize][OpCode::SWAP6.to_usize()] = 3;
    table[Revision::Frontier as usize][OpCode::SWAP7.to_usize()] = 3;
    table[Revision::Frontier as usize][OpCode::SWAP8.to_usize()] = 3;
    table[Revision::Frontier as usize][OpCode::SWAP9.to_usize()] = 3;
    table[Revision::Frontier as usize][OpCode::SWAP10.to_usize()] = 3;
    table[Revision::Frontier as usize][OpCode::SWAP11.to_usize()] = 3;
    table[Revision::Frontier as usize][OpCode::SWAP12.to_usize()] = 3;
    table[Revision::Frontier as usize][OpCode::SWAP13.to_usize()] = 3;
    table[Revision::Frontier as usize][OpCode::SWAP14.to_usize()] = 3;
    table[Revision::Frontier as usize][OpCode::SWAP15.to_usize()] = 3;
    table[Revision::Frontier as usize][OpCode::SWAP16.to_usize()] = 3;

    table[Revision::Frontier as usize][OpCode::LOG0.to_usize()] = 375;
    table[Revision::Frontier as usize][OpCode::LOG1.to_usize()] = 2 * 375;
    table[Revision::Frontier as usize][OpCode::LOG2.to_usize()] = 3 * 375;
    table[Revision::Frontier as usize][OpCode::LOG3.to_usize()] = 4 * 375;
    table[Revision::Frontier as usize][OpCode::LOG4.to_usize()] = 5 * 375;

    table[Revision::Frontier as usize][OpCode::CREATE.to_usize()] = 32000;
    table[Revision::Frontier as usize][OpCode::CALL.to_usize()] = 40;
    table[Revision::Frontier as usize][OpCode::CALLCODE.to_usize()] = 40;
    table[Revision::Frontier as usize][OpCode::RETURN.to_usize()] = 0;
    table[Revision::Frontier as usize][OpCode::INVALID.to_usize()] = 0;
    table[Revision::Frontier as usize][OpCode::SELFDESTRUCT.to_usize()] = 0;

    table[Revision::Homestead as usize] = table[Revision::Frontier as usize];
    table[Revision::Homestead as usize][OpCode::DELEGATECALL.to_usize()] = 40;

    table[Revision::Tangerine as usize] = table[Revision::Homestead as usize];
    table[Revision::Tangerine as usize][OpCode::BALANCE.to_usize()] = 400;
    table[Revision::Tangerine as usize][OpCode::EXTCODESIZE.to_usize()] = 700;
    table[Revision::Tangerine as usize][OpCode::EXTCODECOPY.to_usize()] = 700;
    table[Revision::Tangerine as usize][OpCode::SLOAD.to_usize()] = 200;
    table[Revision::Tangerine as usize][OpCode::CALL.to_usize()] = 700;
    table[Revision::Tangerine as usize][OpCode::CALLCODE.to_usize()] = 700;
    table[Revision::Tangerine as usize][OpCode::DELEGATECALL.to_usize()] = 700;
    table[Revision::Tangerine as usize][OpCode::SELFDESTRUCT.to_usize()] = 5000;

    table[Revision::Spurious as usize] = table[Revision::Tangerine as usize];

    table[Revision::Byzantium as usize] = table[Revision::Spurious as usize];
    table[Revision::Byzantium as usize][OpCode::RETURNDATASIZE.to_usize()] = 2;
    table[Revision::Byzantium as usize][OpCode::RETURNDATACOPY.to_usize()] = 3;
    table[Revision::Byzantium as usize][OpCode::STATICCALL.to_usize()] = 700;
    table[Revision::Byzantium as usize][OpCode::REVERT.to_usize()] = 0;

    table[Revision::Constantinople as usize] = table[Revision::Byzantium as usize];
    table[Revision::Constantinople as usize][OpCode::SHL.to_usize()] = 3;
    table[Revision::Constantinople as usize][OpCode::SHR.to_usize()] = 3;
    table[Revision::Constantinople as usize][OpCode::SAR.to_usize()] = 3;
    table[Revision::Constantinople as usize][OpCode::EXTCODEHASH.to_usize()] = 400;
    table[Revision::Constantinople as usize][OpCode::CREATE2.to_usize()] = 32000;

    table[Revision::Petersburg as usize] = table[Revision::Constantinople as usize];

    table[Revision::Istanbul as usize] = table[Revision::Petersburg as usize];
    table[Revision::Istanbul as usize][OpCode::BALANCE.to_usize()] = 700;
    table[Revision::Istanbul as usize][OpCode::CHAINID.to_usize()] = 2;
    table[Revision::Istanbul as usize][OpCode::EXTCODEHASH.to_usize()] = 700;
    table[Revision::Istanbul as usize][OpCode::SELFBALANCE.to_usize()] = 5;
    table[Revision::Istanbul as usize][OpCode::SLOAD.to_usize()] = 800;

    table[Revision::Berlin as usize] = table[Revision::Istanbul as usize];
    table[Revision::Berlin as usize][OpCode::EXTCODESIZE.to_usize()] =
        WARM_STORAGE_READ_COST as i16;
    table[Revision::Berlin as usize][OpCode::EXTCODECOPY.to_usize()] =
        WARM_STORAGE_READ_COST as i16;
    table[Revision::Berlin as usize][OpCode::EXTCODEHASH.to_usize()] =
        WARM_STORAGE_READ_COST as i16;
    table[Revision::Berlin as usize][OpCode::BALANCE.to_usize()] = WARM_STORAGE_READ_COST as i16;
    table[Revision::Berlin as usize][OpCode::CALL.to_usize()] = WARM_STORAGE_READ_COST as i16;
    table[Revision::Berlin as usize][OpCode::CALLCODE.to_usize()] = WARM_STORAGE_READ_COST as i16;
    table[Revision::Berlin as usize][OpCode::DELEGATECALL.to_usize()] =
        WARM_STORAGE_READ_COST as i16;
    table[Revision::Berlin as usize][OpCode::STATICCALL.to_usize()] = WARM_STORAGE_READ_COST as i16;
    table[Revision::Berlin as usize][OpCode::SLOAD.to_usize()] = WARM_STORAGE_READ_COST as i16;

    table[Revision::London as usize] = table[Revision::Berlin as usize];
    table[Revision::London as usize][OpCode::BASEFEE.to_usize()] = 2;

    table[Revision::Paris as usize] = table[Revision::London as usize];

    table
}

pub const GAS_COSTS: GasCostTable = gas_costs();

pub const fn has_const_gas_cost<const OPCODE: OpCode>() -> bool {
    const LATEST: Revision = Revision::latest();

    let g = GAS_COSTS[Revision::Frontier as usize][OPCODE.to_usize()];
    let revtable = Revision::iter();
    let mut iter = 0;
    loop {
        let rev = revtable[iter];

        if GAS_COSTS[rev as usize][OPCODE.to_usize()] != g {
            return false;
        }

        if matches!(rev, LATEST) {
            break;
        } else {
            iter += 1;
        }
    }
    true
}

pub const fn opcode_gas_cost<const REVISION: Revision, const OPCODE: OpCode>() -> i16 {
    GAS_COSTS[REVISION as usize][OPCODE.to_usize()]
}

const fn property_table() -> [Option<Properties>; 256] {
    let mut table = [None; 256];

    table[OpCode::STOP.to_usize()] = Some(Properties::new(0, 0));
    table[OpCode::ADD.to_usize()] = Some(Properties::new(2, -1));
    table[OpCode::MUL.to_usize()] = Some(Properties::new(2, -1));
    table[OpCode::SUB.to_usize()] = Some(Properties::new(2, -1));
    table[OpCode::DIV.to_usize()] = Some(Properties::new(2, -1));
    table[OpCode::SDIV.to_usize()] = Some(Properties::new(2, -1));
    table[OpCode::MOD.to_usize()] = Some(Properties::new(2, -1));
    table[OpCode::SMOD.to_usize()] = Some(Properties::new(2, -1));
    table[OpCode::ADDMOD.to_usize()] = Some(Properties::new(3, -2));
    table[OpCode::MULMOD.to_usize()] = Some(Properties::new(3, -2));
    table[OpCode::EXP.to_usize()] = Some(Properties::new(2, -1));
    table[OpCode::SIGNEXTEND.to_usize()] = Some(Properties::new(2, -1));

    table[OpCode::LT.to_usize()] = Some(Properties::new(2, -1));
    table[OpCode::GT.to_usize()] = Some(Properties::new(2, -1));
    table[OpCode::SLT.to_usize()] = Some(Properties::new(2, -1));
    table[OpCode::SGT.to_usize()] = Some(Properties::new(2, -1));
    table[OpCode::EQ.to_usize()] = Some(Properties::new(2, -1));
    table[OpCode::ISZERO.to_usize()] = Some(Properties::new(1, 0));
    table[OpCode::AND.to_usize()] = Some(Properties::new(2, -1));
    table[OpCode::OR.to_usize()] = Some(Properties::new(2, -1));
    table[OpCode::XOR.to_usize()] = Some(Properties::new(2, -1));
    table[OpCode::NOT.to_usize()] = Some(Properties::new(1, 0));
    table[OpCode::BYTE.to_usize()] = Some(Properties::new(2, -1));
    table[OpCode::SHL.to_usize()] = Some(Properties::new(2, -1));
    table[OpCode::SHR.to_usize()] = Some(Properties::new(2, -1));
    table[OpCode::SAR.to_usize()] = Some(Properties::new(2, -1));

    table[OpCode::KECCAK256.to_usize()] = Some(Properties::new(2, -1));

    table[OpCode::ADDRESS.to_usize()] = Some(Properties::new(0, 1));
    table[OpCode::BALANCE.to_usize()] = Some(Properties::new(1, 0));
    table[OpCode::ORIGIN.to_usize()] = Some(Properties::new(0, 1));
    table[OpCode::CALLER.to_usize()] = Some(Properties::new(0, 1));
    table[OpCode::CALLVALUE.to_usize()] = Some(Properties::new(0, 1));
    table[OpCode::CALLDATALOAD.to_usize()] = Some(Properties::new(1, 0));
    table[OpCode::CALLDATASIZE.to_usize()] = Some(Properties::new(0, 1));
    table[OpCode::CALLDATACOPY.to_usize()] = Some(Properties::new(3, -3));
    table[OpCode::CODESIZE.to_usize()] = Some(Properties::new(0, 1));
    table[OpCode::CODECOPY.to_usize()] = Some(Properties::new(3, -3));
    table[OpCode::GASPRICE.to_usize()] = Some(Properties::new(0, 1));
    table[OpCode::EXTCODESIZE.to_usize()] = Some(Properties::new(1, 0));
    table[OpCode::EXTCODECOPY.to_usize()] = Some(Properties::new(4, -4));
    table[OpCode::RETURNDATASIZE.to_usize()] = Some(Properties::new(0, 1));
    table[OpCode::RETURNDATACOPY.to_usize()] = Some(Properties::new(3, -3));
    table[OpCode::EXTCODEHASH.to_usize()] = Some(Properties::new(1, 0));

    table[OpCode::BLOCKHASH.to_usize()] = Some(Properties::new(1, 0));
    table[OpCode::COINBASE.to_usize()] = Some(Properties::new(0, 1));
    table[OpCode::TIMESTAMP.to_usize()] = Some(Properties::new(0, 1));
    table[OpCode::NUMBER.to_usize()] = Some(Properties::new(0, 1));
    table[OpCode::DIFFICULTY.to_usize()] = Some(Properties::new(0, 1));
    table[OpCode::GASLIMIT.to_usize()] = Some(Properties::new(0, 1));
    table[OpCode::CHAINID.to_usize()] = Some(Properties::new(0, 1));
    table[OpCode::SELFBALANCE.to_usize()] = Some(Properties::new(0, 1));
    table[OpCode::BASEFEE.to_usize()] = Some(Properties::new(0, 1));

    table[OpCode::POP.to_usize()] = Some(Properties::new(1, -1));
    table[OpCode::MLOAD.to_usize()] = Some(Properties::new(1, 0));
    table[OpCode::MSTORE.to_usize()] = Some(Properties::new(2, -2));
    table[OpCode::MSTORE8.to_usize()] = Some(Properties::new(2, -2));
    table[OpCode::SLOAD.to_usize()] = Some(Properties::new(1, 0));
    table[OpCode::SSTORE.to_usize()] = Some(Properties::new(2, -2));
    table[OpCode::JUMP.to_usize()] = Some(Properties::new(1, -1));
    table[OpCode::JUMPI.to_usize()] = Some(Properties::new(2, -2));
    table[OpCode::PC.to_usize()] = Some(Properties::new(0, 1));
    table[OpCode::MSIZE.to_usize()] = Some(Properties::new(0, 1));
    table[OpCode::GAS.to_usize()] = Some(Properties::new(0, 1));
    table[OpCode::JUMPDEST.to_usize()] = Some(Properties::new(0, 0));

    table[OpCode::PUSH1.to_usize()] = Some(Properties::new(0, 1));
    table[OpCode::PUSH2.to_usize()] = Some(Properties::new(0, 1));
    table[OpCode::PUSH3.to_usize()] = Some(Properties::new(0, 1));
    table[OpCode::PUSH4.to_usize()] = Some(Properties::new(0, 1));
    table[OpCode::PUSH5.to_usize()] = Some(Properties::new(0, 1));
    table[OpCode::PUSH6.to_usize()] = Some(Properties::new(0, 1));
    table[OpCode::PUSH7.to_usize()] = Some(Properties::new(0, 1));
    table[OpCode::PUSH8.to_usize()] = Some(Properties::new(0, 1));
    table[OpCode::PUSH9.to_usize()] = Some(Properties::new(0, 1));
    table[OpCode::PUSH10.to_usize()] = Some(Properties::new(0, 1));
    table[OpCode::PUSH11.to_usize()] = Some(Properties::new(0, 1));
    table[OpCode::PUSH12.to_usize()] = Some(Properties::new(0, 1));
    table[OpCode::PUSH13.to_usize()] = Some(Properties::new(0, 1));
    table[OpCode::PUSH14.to_usize()] = Some(Properties::new(0, 1));
    table[OpCode::PUSH15.to_usize()] = Some(Properties::new(0, 1));
    table[OpCode::PUSH16.to_usize()] = Some(Properties::new(0, 1));
    table[OpCode::PUSH17.to_usize()] = Some(Properties::new(0, 1));
    table[OpCode::PUSH18.to_usize()] = Some(Properties::new(0, 1));
    table[OpCode::PUSH19.to_usize()] = Some(Properties::new(0, 1));
    table[OpCode::PUSH20.to_usize()] = Some(Properties::new(0, 1));
    table[OpCode::PUSH21.to_usize()] = Some(Properties::new(0, 1));
    table[OpCode::PUSH22.to_usize()] = Some(Properties::new(0, 1));
    table[OpCode::PUSH23.to_usize()] = Some(Properties::new(0, 1));
    table[OpCode::PUSH24.to_usize()] = Some(Properties::new(0, 1));
    table[OpCode::PUSH25.to_usize()] = Some(Properties::new(0, 1));
    table[OpCode::PUSH26.to_usize()] = Some(Properties::new(0, 1));
    table[OpCode::PUSH27.to_usize()] = Some(Properties::new(0, 1));
    table[OpCode::PUSH28.to_usize()] = Some(Properties::new(0, 1));
    table[OpCode::PUSH29.to_usize()] = Some(Properties::new(0, 1));
    table[OpCode::PUSH30.to_usize()] = Some(Properties::new(0, 1));
    table[OpCode::PUSH31.to_usize()] = Some(Properties::new(0, 1));
    table[OpCode::PUSH32.to_usize()] = Some(Properties::new(0, 1));

    table[OpCode::DUP1.to_usize()] = Some(Properties::new(1, 1));
    table[OpCode::DUP2.to_usize()] = Some(Properties::new(2, 1));
    table[OpCode::DUP3.to_usize()] = Some(Properties::new(3, 1));
    table[OpCode::DUP4.to_usize()] = Some(Properties::new(4, 1));
    table[OpCode::DUP5.to_usize()] = Some(Properties::new(5, 1));
    table[OpCode::DUP6.to_usize()] = Some(Properties::new(6, 1));
    table[OpCode::DUP7.to_usize()] = Some(Properties::new(7, 1));
    table[OpCode::DUP8.to_usize()] = Some(Properties::new(8, 1));
    table[OpCode::DUP9.to_usize()] = Some(Properties::new(9, 1));
    table[OpCode::DUP10.to_usize()] = Some(Properties::new(10, 1));
    table[OpCode::DUP11.to_usize()] = Some(Properties::new(11, 1));
    table[OpCode::DUP12.to_usize()] = Some(Properties::new(12, 1));
    table[OpCode::DUP13.to_usize()] = Some(Properties::new(13, 1));
    table[OpCode::DUP14.to_usize()] = Some(Properties::new(14, 1));
    table[OpCode::DUP15.to_usize()] = Some(Properties::new(15, 1));
    table[OpCode::DUP16.to_usize()] = Some(Properties::new(16, 1));

    table[OpCode::SWAP1.to_usize()] = Some(Properties::new(2, 0));
    table[OpCode::SWAP2.to_usize()] = Some(Properties::new(3, 0));
    table[OpCode::SWAP3.to_usize()] = Some(Properties::new(4, 0));
    table[OpCode::SWAP4.to_usize()] = Some(Properties::new(5, 0));
    table[OpCode::SWAP5.to_usize()] = Some(Properties::new(6, 0));
    table[OpCode::SWAP6.to_usize()] = Some(Properties::new(7, 0));
    table[OpCode::SWAP7.to_usize()] = Some(Properties::new(8, 0));
    table[OpCode::SWAP8.to_usize()] = Some(Properties::new(9, 0));
    table[OpCode::SWAP9.to_usize()] = Some(Properties::new(10, 0));
    table[OpCode::SWAP10.to_usize()] = Some(Properties::new(11, 0));
    table[OpCode::SWAP11.to_usize()] = Some(Properties::new(12, 0));
    table[OpCode::SWAP12.to_usize()] = Some(Properties::new(13, 0));
    table[OpCode::SWAP13.to_usize()] = Some(Properties::new(14, 0));
    table[OpCode::SWAP14.to_usize()] = Some(Properties::new(15, 0));
    table[OpCode::SWAP15.to_usize()] = Some(Properties::new(16, 0));
    table[OpCode::SWAP16.to_usize()] = Some(Properties::new(17, 0));

    table[OpCode::LOG0.to_usize()] = Some(Properties::new(2, -2));
    table[OpCode::LOG1.to_usize()] = Some(Properties::new(3, -3));
    table[OpCode::LOG2.to_usize()] = Some(Properties::new(4, -4));
    table[OpCode::LOG3.to_usize()] = Some(Properties::new(5, -5));
    table[OpCode::LOG4.to_usize()] = Some(Properties::new(6, -6));

    table[OpCode::CREATE.to_usize()] = Some(Properties::new(3, -2));
    table[OpCode::CALL.to_usize()] = Some(Properties::new(7, -6));
    table[OpCode::CALLCODE.to_usize()] = Some(Properties::new(7, -6));
    table[OpCode::RETURN.to_usize()] = Some(Properties::new(2, -2));
    table[OpCode::DELEGATECALL.to_usize()] = Some(Properties::new(6, -5));
    table[OpCode::CREATE2.to_usize()] = Some(Properties::new(4, -3));
    table[OpCode::STATICCALL.to_usize()] = Some(Properties::new(6, -5));
    table[OpCode::REVERT.to_usize()] = Some(Properties::new(2, -2));
    table[OpCode::INVALID.to_usize()] = Some(Properties::new(0, 0));
    table[OpCode::SELFDESTRUCT.to_usize()] = Some(Properties::new(1, -1));

    table
}

pub const PROPERTIES: [Option<Properties>; 256] = property_table();

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn const_gas_cost() {
        assert!(has_const_gas_cost::<{ OpCode::STOP }>());
        assert!(has_const_gas_cost::<{ OpCode::ADD }>());
        assert!(has_const_gas_cost::<{ OpCode::PUSH1 }>());
        assert!(!has_const_gas_cost::<{ OpCode::SHL }>());
        assert!(!has_const_gas_cost::<{ OpCode::BALANCE }>());
        assert!(!has_const_gas_cost::<{ OpCode::SLOAD }>());
    }
}
