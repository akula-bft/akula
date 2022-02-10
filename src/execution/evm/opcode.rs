use std::{borrow::Cow, fmt::Display};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(transparent)]
pub struct OpCode(pub u8);

impl OpCode {
    #[inline(always)]
    pub const fn to_u8(self) -> u8 {
        self.0
    }

    #[inline(always)]
    pub const fn to_usize(self) -> usize {
        self.to_u8() as usize
    }
}

impl OpCode {
    pub const STOP: OpCode = OpCode(0x00);
    pub const ADD: OpCode = OpCode(0x01);
    pub const MUL: OpCode = OpCode(0x02);
    pub const SUB: OpCode = OpCode(0x03);
    pub const DIV: OpCode = OpCode(0x04);
    pub const SDIV: OpCode = OpCode(0x05);
    pub const MOD: OpCode = OpCode(0x06);
    pub const SMOD: OpCode = OpCode(0x07);
    pub const ADDMOD: OpCode = OpCode(0x08);
    pub const MULMOD: OpCode = OpCode(0x09);
    pub const EXP: OpCode = OpCode(0x0a);
    pub const SIGNEXTEND: OpCode = OpCode(0x0b);

    pub const LT: OpCode = OpCode(0x10);
    pub const GT: OpCode = OpCode(0x11);
    pub const SLT: OpCode = OpCode(0x12);
    pub const SGT: OpCode = OpCode(0x13);
    pub const EQ: OpCode = OpCode(0x14);
    pub const ISZERO: OpCode = OpCode(0x15);
    pub const AND: OpCode = OpCode(0x16);
    pub const OR: OpCode = OpCode(0x17);
    pub const XOR: OpCode = OpCode(0x18);
    pub const NOT: OpCode = OpCode(0x19);
    pub const BYTE: OpCode = OpCode(0x1a);
    pub const SHL: OpCode = OpCode(0x1b);
    pub const SHR: OpCode = OpCode(0x1c);
    pub const SAR: OpCode = OpCode(0x1d);

    pub const KECCAK256: OpCode = OpCode(0x20);

    pub const ADDRESS: OpCode = OpCode(0x30);
    pub const BALANCE: OpCode = OpCode(0x31);
    pub const ORIGIN: OpCode = OpCode(0x32);
    pub const CALLER: OpCode = OpCode(0x33);
    pub const CALLVALUE: OpCode = OpCode(0x34);
    pub const CALLDATALOAD: OpCode = OpCode(0x35);
    pub const CALLDATASIZE: OpCode = OpCode(0x36);
    pub const CALLDATACOPY: OpCode = OpCode(0x37);
    pub const CODESIZE: OpCode = OpCode(0x38);
    pub const CODECOPY: OpCode = OpCode(0x39);
    pub const GASPRICE: OpCode = OpCode(0x3a);
    pub const EXTCODESIZE: OpCode = OpCode(0x3b);
    pub const EXTCODECOPY: OpCode = OpCode(0x3c);
    pub const RETURNDATASIZE: OpCode = OpCode(0x3d);
    pub const RETURNDATACOPY: OpCode = OpCode(0x3e);
    pub const EXTCODEHASH: OpCode = OpCode(0x3f);

    pub const BLOCKHASH: OpCode = OpCode(0x40);
    pub const COINBASE: OpCode = OpCode(0x41);
    pub const TIMESTAMP: OpCode = OpCode(0x42);
    pub const NUMBER: OpCode = OpCode(0x43);
    pub const DIFFICULTY: OpCode = OpCode(0x44);
    pub const GASLIMIT: OpCode = OpCode(0x45);
    pub const CHAINID: OpCode = OpCode(0x46);
    pub const SELFBALANCE: OpCode = OpCode(0x47);
    pub const BASEFEE: OpCode = OpCode(0x48);

    pub const POP: OpCode = OpCode(0x50);
    pub const MLOAD: OpCode = OpCode(0x51);
    pub const MSTORE: OpCode = OpCode(0x52);
    pub const MSTORE8: OpCode = OpCode(0x53);
    pub const SLOAD: OpCode = OpCode(0x54);
    pub const SSTORE: OpCode = OpCode(0x55);
    pub const JUMP: OpCode = OpCode(0x56);
    pub const JUMPI: OpCode = OpCode(0x57);
    pub const PC: OpCode = OpCode(0x58);
    pub const MSIZE: OpCode = OpCode(0x59);
    pub const GAS: OpCode = OpCode(0x5a);
    pub const JUMPDEST: OpCode = OpCode(0x5b);

    pub const PUSH1: OpCode = OpCode(0x60);
    pub const PUSH2: OpCode = OpCode(0x61);
    pub const PUSH3: OpCode = OpCode(0x62);
    pub const PUSH4: OpCode = OpCode(0x63);
    pub const PUSH5: OpCode = OpCode(0x64);
    pub const PUSH6: OpCode = OpCode(0x65);
    pub const PUSH7: OpCode = OpCode(0x66);
    pub const PUSH8: OpCode = OpCode(0x67);
    pub const PUSH9: OpCode = OpCode(0x68);
    pub const PUSH10: OpCode = OpCode(0x69);
    pub const PUSH11: OpCode = OpCode(0x6a);
    pub const PUSH12: OpCode = OpCode(0x6b);
    pub const PUSH13: OpCode = OpCode(0x6c);
    pub const PUSH14: OpCode = OpCode(0x6d);
    pub const PUSH15: OpCode = OpCode(0x6e);
    pub const PUSH16: OpCode = OpCode(0x6f);
    pub const PUSH17: OpCode = OpCode(0x70);
    pub const PUSH18: OpCode = OpCode(0x71);
    pub const PUSH19: OpCode = OpCode(0x72);
    pub const PUSH20: OpCode = OpCode(0x73);
    pub const PUSH21: OpCode = OpCode(0x74);
    pub const PUSH22: OpCode = OpCode(0x75);
    pub const PUSH23: OpCode = OpCode(0x76);
    pub const PUSH24: OpCode = OpCode(0x77);
    pub const PUSH25: OpCode = OpCode(0x78);
    pub const PUSH26: OpCode = OpCode(0x79);
    pub const PUSH27: OpCode = OpCode(0x7a);
    pub const PUSH28: OpCode = OpCode(0x7b);
    pub const PUSH29: OpCode = OpCode(0x7c);
    pub const PUSH30: OpCode = OpCode(0x7d);
    pub const PUSH31: OpCode = OpCode(0x7e);
    pub const PUSH32: OpCode = OpCode(0x7f);
    pub const DUP1: OpCode = OpCode(0x80);
    pub const DUP2: OpCode = OpCode(0x81);
    pub const DUP3: OpCode = OpCode(0x82);
    pub const DUP4: OpCode = OpCode(0x83);
    pub const DUP5: OpCode = OpCode(0x84);
    pub const DUP6: OpCode = OpCode(0x85);
    pub const DUP7: OpCode = OpCode(0x86);
    pub const DUP8: OpCode = OpCode(0x87);
    pub const DUP9: OpCode = OpCode(0x88);
    pub const DUP10: OpCode = OpCode(0x89);
    pub const DUP11: OpCode = OpCode(0x8a);
    pub const DUP12: OpCode = OpCode(0x8b);
    pub const DUP13: OpCode = OpCode(0x8c);
    pub const DUP14: OpCode = OpCode(0x8d);
    pub const DUP15: OpCode = OpCode(0x8e);
    pub const DUP16: OpCode = OpCode(0x8f);
    pub const SWAP1: OpCode = OpCode(0x90);
    pub const SWAP2: OpCode = OpCode(0x91);
    pub const SWAP3: OpCode = OpCode(0x92);
    pub const SWAP4: OpCode = OpCode(0x93);
    pub const SWAP5: OpCode = OpCode(0x94);
    pub const SWAP6: OpCode = OpCode(0x95);
    pub const SWAP7: OpCode = OpCode(0x96);
    pub const SWAP8: OpCode = OpCode(0x97);
    pub const SWAP9: OpCode = OpCode(0x98);
    pub const SWAP10: OpCode = OpCode(0x99);
    pub const SWAP11: OpCode = OpCode(0x9a);
    pub const SWAP12: OpCode = OpCode(0x9b);
    pub const SWAP13: OpCode = OpCode(0x9c);
    pub const SWAP14: OpCode = OpCode(0x9d);
    pub const SWAP15: OpCode = OpCode(0x9e);
    pub const SWAP16: OpCode = OpCode(0x9f);
    pub const LOG0: OpCode = OpCode(0xa0);
    pub const LOG1: OpCode = OpCode(0xa1);
    pub const LOG2: OpCode = OpCode(0xa2);
    pub const LOG3: OpCode = OpCode(0xa3);
    pub const LOG4: OpCode = OpCode(0xa4);

    pub const CREATE: OpCode = OpCode(0xf0);
    pub const CALL: OpCode = OpCode(0xf1);
    pub const CALLCODE: OpCode = OpCode(0xf2);
    pub const RETURN: OpCode = OpCode(0xf3);
    pub const DELEGATECALL: OpCode = OpCode(0xf4);
    pub const CREATE2: OpCode = OpCode(0xf5);

    pub const STATICCALL: OpCode = OpCode(0xfa);

    pub const REVERT: OpCode = OpCode(0xfd);
    pub const INVALID: OpCode = OpCode(0xfe);
    pub const SELFDESTRUCT: OpCode = OpCode(0xff);
}

impl OpCode {
    pub const fn name(&self) -> &'static str {
        match *self {
            OpCode::STOP => "STOP",
            OpCode::ADD => "ADD",
            OpCode::MUL => "MUL",
            OpCode::SUB => "SUB",
            OpCode::DIV => "DIV",
            OpCode::SDIV => "SDIV",
            OpCode::MOD => "MOD",
            OpCode::SMOD => "SMOD",
            OpCode::ADDMOD => "ADDMOD",
            OpCode::MULMOD => "MULMOD",
            OpCode::EXP => "EXP",
            OpCode::SIGNEXTEND => "SIGNEXTEND",
            OpCode::LT => "LT",
            OpCode::GT => "GT",
            OpCode::SLT => "SLT",
            OpCode::SGT => "SGT",
            OpCode::EQ => "EQ",
            OpCode::ISZERO => "ISZERO",
            OpCode::AND => "AND",
            OpCode::OR => "OR",
            OpCode::XOR => "XOR",
            OpCode::NOT => "NOT",
            OpCode::BYTE => "BYTE",
            OpCode::SHL => "SHL",
            OpCode::SHR => "SHR",
            OpCode::SAR => "SAR",
            OpCode::KECCAK256 => "KECCAK256",
            OpCode::ADDRESS => "ADDRESS",
            OpCode::BALANCE => "BALANCE",
            OpCode::ORIGIN => "ORIGIN",
            OpCode::CALLER => "CALLER",
            OpCode::CALLVALUE => "CALLVALUE",
            OpCode::CALLDATALOAD => "CALLDATALOAD",
            OpCode::CALLDATASIZE => "CALLDATASIZE",
            OpCode::CALLDATACOPY => "CALLDATACOPY",
            OpCode::CODESIZE => "CODESIZE",
            OpCode::CODECOPY => "CODECOPY",
            OpCode::GASPRICE => "GASPRICE",
            OpCode::EXTCODESIZE => "EXTCODESIZE",
            OpCode::EXTCODECOPY => "EXTCODECOPY",
            OpCode::RETURNDATASIZE => "RETURNDATASIZE",
            OpCode::RETURNDATACOPY => "RETURNDATACOPY",
            OpCode::EXTCODEHASH => "EXTCODEHASH",
            OpCode::BLOCKHASH => "BLOCKHASH",
            OpCode::COINBASE => "COINBASE",
            OpCode::TIMESTAMP => "TIMESTAMP",
            OpCode::NUMBER => "NUMBER",
            OpCode::DIFFICULTY => "DIFFICULTY",
            OpCode::GASLIMIT => "GASLIMIT",
            OpCode::CHAINID => "CHAINID",
            OpCode::SELFBALANCE => "SELFBALANCE",
            OpCode::BASEFEE => "BASEFEE",
            OpCode::POP => "POP",
            OpCode::MLOAD => "MLOAD",
            OpCode::MSTORE => "MSTORE",
            OpCode::MSTORE8 => "MSTORE8",
            OpCode::SLOAD => "SLOAD",
            OpCode::SSTORE => "SSTORE",
            OpCode::JUMP => "JUMP",
            OpCode::JUMPI => "JUMPI",
            OpCode::PC => "PC",
            OpCode::MSIZE => "MSIZE",
            OpCode::GAS => "GAS",
            OpCode::JUMPDEST => "JUMPDEST",
            OpCode::PUSH1 => "PUSH1",
            OpCode::PUSH2 => "PUSH2",
            OpCode::PUSH3 => "PUSH3",
            OpCode::PUSH4 => "PUSH4",
            OpCode::PUSH5 => "PUSH5",
            OpCode::PUSH6 => "PUSH6",
            OpCode::PUSH7 => "PUSH7",
            OpCode::PUSH8 => "PUSH8",
            OpCode::PUSH9 => "PUSH9",
            OpCode::PUSH10 => "PUSH10",
            OpCode::PUSH11 => "PUSH11",
            OpCode::PUSH12 => "PUSH12",
            OpCode::PUSH13 => "PUSH13",
            OpCode::PUSH14 => "PUSH14",
            OpCode::PUSH15 => "PUSH15",
            OpCode::PUSH16 => "PUSH16",
            OpCode::PUSH17 => "PUSH17",
            OpCode::PUSH18 => "PUSH18",
            OpCode::PUSH19 => "PUSH19",
            OpCode::PUSH20 => "PUSH20",
            OpCode::PUSH21 => "PUSH21",
            OpCode::PUSH22 => "PUSH22",
            OpCode::PUSH23 => "PUSH23",
            OpCode::PUSH24 => "PUSH24",
            OpCode::PUSH25 => "PUSH25",
            OpCode::PUSH26 => "PUSH26",
            OpCode::PUSH27 => "PUSH27",
            OpCode::PUSH28 => "PUSH28",
            OpCode::PUSH29 => "PUSH29",
            OpCode::PUSH30 => "PUSH30",
            OpCode::PUSH31 => "PUSH31",
            OpCode::PUSH32 => "PUSH32",
            OpCode::DUP1 => "DUP1",
            OpCode::DUP2 => "DUP2",
            OpCode::DUP3 => "DUP3",
            OpCode::DUP4 => "DUP4",
            OpCode::DUP5 => "DUP5",
            OpCode::DUP6 => "DUP6",
            OpCode::DUP7 => "DUP7",
            OpCode::DUP8 => "DUP8",
            OpCode::DUP9 => "DUP9",
            OpCode::DUP10 => "DUP10",
            OpCode::DUP11 => "DUP11",
            OpCode::DUP12 => "DUP12",
            OpCode::DUP13 => "DUP13",
            OpCode::DUP14 => "DUP14",
            OpCode::DUP15 => "DUP15",
            OpCode::DUP16 => "DUP16",
            OpCode::SWAP1 => "SWAP1",
            OpCode::SWAP2 => "SWAP2",
            OpCode::SWAP3 => "SWAP3",
            OpCode::SWAP4 => "SWAP4",
            OpCode::SWAP5 => "SWAP5",
            OpCode::SWAP6 => "SWAP6",
            OpCode::SWAP7 => "SWAP7",
            OpCode::SWAP8 => "SWAP8",
            OpCode::SWAP9 => "SWAP9",
            OpCode::SWAP10 => "SWAP10",
            OpCode::SWAP11 => "SWAP11",
            OpCode::SWAP12 => "SWAP12",
            OpCode::SWAP13 => "SWAP13",
            OpCode::SWAP14 => "SWAP14",
            OpCode::SWAP15 => "SWAP15",
            OpCode::SWAP16 => "SWAP16",
            OpCode::LOG0 => "LOG0",
            OpCode::LOG1 => "LOG1",
            OpCode::LOG2 => "LOG2",
            OpCode::LOG3 => "LOG3",
            OpCode::LOG4 => "LOG4",
            OpCode::CREATE => "CREATE",
            OpCode::CALL => "CALL",
            OpCode::CALLCODE => "CALLCODE",
            OpCode::RETURN => "RETURN",
            OpCode::DELEGATECALL => "DELEGATECALL",
            OpCode::CREATE2 => "CREATE2",
            OpCode::STATICCALL => "STATICCALL",
            OpCode::REVERT => "REVERT",
            OpCode::INVALID => "INVALID",
            OpCode::SELFDESTRUCT => "SELFDESTRUCT",
            _ => "UNDEFINED",
        }
    }

    #[cfg(feature = "util")]
    #[inline(always)]
    pub fn push_size(self) -> Option<u8> {
        (self.to_u8() >= OpCode::PUSH1.to_u8() && self.to_u8() <= OpCode::PUSH32.to_u8())
            .then(|| self.to_u8() - OpCode::PUSH1.to_u8() + 1)
    }
}

impl Display for OpCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = self.name();

        let n = if name == "UNDEFINED" {
            Cow::Owned(format!("UNDEFINED(0x{:02x})", self.0))
        } else {
            Cow::Borrowed(name)
        };
        write!(f, "{}", n)
    }
}
