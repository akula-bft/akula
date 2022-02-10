#![allow(clippy::needless_range_loop)]

use crate::{
    execution::evm::{opcode::*, util::*, *},
    models::*,
};
use core::iter::repeat;
use ethnum::U256;
use hex_literal::hex;
use std::cmp::max;

#[test]
fn empty_code() {
    for gas in [0, 1] {
        EvmTester::new()
            .code(hex!(""))
            .gas(gas)
            .gas_used(0)
            .status(StatusCode::Success)
            .check()
    }
}

#[test]
fn invalid_push() {
    EvmTester::new()
        .code(Bytecode::new().opcode(OpCode::PUSH1))
        .status(StatusCode::Success)
        .check();
}

#[test]
fn push_and_pop() {
    EvmTester::new()
        .code(
            Bytecode::new()
                .pushb(hex!("0102"))
                .opcode(OpCode::POP)
                .pushb(hex!("010203040506070809"))
                .opcode(OpCode::POP),
        )
        .gas(11)
        .gas_used(10)
        .status(StatusCode::Success)
        .check()
}

#[test]
fn stack_underflow() {
    EvmTester::new()
        .code(
            Bytecode::new()
                .pushv(1)
                .opcode(OpCode::POP)
                .pushv(1)
                .opcode(OpCode::POP)
                .opcode(OpCode::POP),
        )
        .gas(13)
        .status(StatusCode::StackUnderflow)
        .check();

    EvmTester::new()
        .code(Bytecode::new().opcode(OpCode::NOT))
        .status(StatusCode::StackUnderflow)
        .check();
}

#[test]
fn add() {
    EvmTester::new()
        .code(hex!("6007600d0160005260206000f3"))
        .gas(25)
        .gas_used(24)
        .status(StatusCode::Success)
        .output_value(20)
        .check()
}

#[test]
fn dup() {
    // 0 7 3 5
    // 0 7 3 5 3 5
    // 0 7 3 5 3 5 5 7
    // 0 7 3 5 20
    // 0 7 3 5 (20 0)
    // 0 7 3 5 3 0
    EvmTester::new()
        .code(hex!("6000600760036005818180850101018452602084f3"))
        .gas(48)
        .status(StatusCode::Success)
        .output_value(20)
        .check()
}

#[test]
fn dup_all_1() {
    EvmTester::new()
        .code(
            Bytecode::new()
                .pushv(1)
                .append(hex!("808182838485868788898a8b8c8d8e8f"))
                .append(hex!("01010101010101010101010101010101"))
                .ret_top(),
        )
        .status(StatusCode::Success)
        .output_value(17)
        .check()
}

#[test]
fn dup_stack_overflow() {
    let b = Bytecode::new()
        .pushv(1)
        .append(hex!("808182838485868788898a8b8c8d8e8f"))
        .append(repeat(0x8f).take(1024 - 17));

    EvmTester::new()
        .code(b.clone())
        .status(StatusCode::Success)
        .check();

    EvmTester::new()
        .code(b.append([0x8f]))
        .status(StatusCode::StackOverflow)
        .check();
}

#[test]
fn dup_stack_underflow() {
    for i in 0..16 {
        EvmTester::new()
            .code(
                Bytecode::new()
                    .pushv(0)
                    .repeat(i)
                    .opcode(OpCode(OpCode::DUP1.0 + i as u8)),
            )
            .status(StatusCode::StackUnderflow)
            .check()
    }
}

#[test]
fn sub_and_swap() {
    EvmTester::new()
        .code(hex!("600180810380829052602090f3"))
        .gas(33)
        .status(StatusCode::Success)
        .gas_left(0)
        .output_value(1)
        .check()
}

#[test]
fn memory_and_not() {
    EvmTester::new()
        .code(hex!("600060018019815381518252800190f3"))
        .gas(42)
        .status(StatusCode::Success)
        .gas_left(0)
        .output_data(hex!("00fe"))
        .check()
}

#[test]
fn msize() {
    EvmTester::new()
        .code(hex!("60aa6022535960005360016000f3"))
        .gas(29)
        .status(StatusCode::Success)
        .gas_left(0)
        .output_data(hex!("40"))
        .check()
}

#[test]
fn gas() {
    EvmTester::new()
        .code(hex!("5a5a5a010160005360016000f3"))
        .gas(40)
        .status(StatusCode::Success)
        .gas_left(13)
        .output_data([38 + 36 + 34])
        .check()
}

#[test]
fn arith() {
    // x = (0 - 1) * 3
    // y = 17 s/ x
    // z = 17 s% x
    // a = 17 * x + z
    // iszero
    EvmTester::new()
        .code(
            Bytecode::new()
                .append(hex!("60116001600003600302")) // 17 -3
                .append(hex!("808205")) // 17 -3 -5
                .append(hex!("818307")) // 17 -3 -5 2
                .append(hex!("910201")) // 17 17
                .append(hex!("0315")) // 1
                .append(hex!("60005360016000f3")),
        )
        .gas(100)
        .status(StatusCode::Success)
        .gas_left(26)
        .output_data([1])
        .check()
}

#[test]
fn comparison() {
    EvmTester::new()
        .code(
            Bytecode::new()
                .append(hex!("60006001808203808001")) // 0 1 -1 -2
                .append(hex!("828210600053")) // m[0] = -1 < 1
                .append(hex!("828211600153")) // m[1] = -1 > 1
                .append(hex!("828212600253")) // m[2] = -1 s< 1
                .append(hex!("828213600353")) // m[3] = -1 s> 1
                .append(hex!("828214600453")) // m[4] = -1 == 1
                .append(hex!("818112600553")) // m[5] = -2 s< -1
                .append(hex!("818113600653")) // m[6] = -2 s> -1
                .append(hex!("60076000f3")),
        )
        .status(StatusCode::Success)
        .gas_used(138)
        .output_data(hex!("00010100000100"))
        .check()
}

#[allow(clippy::identity_op)]
#[test]
fn bitwise() {
    EvmTester::new()
        .code(
            Bytecode::new()
                .append(hex!("60aa60ff")) // aa ff
                .append(hex!("818116600053")) // m[0] = aa & ff
                .append(hex!("818117600153")) // m[1] = aa | ff
                .append(hex!("818118600253")) // m[2] = aa ^ ff
                .append(hex!("60036000f3")),
        )
        .gas(60)
        .gas_left(0)
        .output_data([0xaa & 0xff, 0xaa | 0xff, 0xaa ^ 0xff])
        .check()
}

#[test]
fn jump() {
    EvmTester::new()
        .code(
            Bytecode::new()
                .append(hex!("60be600053")) // m[0] = be
                .append(hex!("60fa")) // fa
                .append(hex!("60055801")) // PC + 5
                .append(hex!("56")) // JUMP
                .append(hex!("5050")) // POP x2
                .append(hex!("5b")) // JUMPDEST
                .append(hex!("600153")) // m[1] = fa
                .append(hex!("60026000f3")), // RETURN(0,2)
        )
        .gas(44)
        .status(StatusCode::Success)
        .gas_left(0)
        .output_data(hex!("befa"))
        .check()
}

#[test]
fn jumpi() {
    EvmTester::new()
        .code(
            Bytecode::new()
                .append(hex!("5a600557")) // GAS 5 JUMPI
                .append(hex!("00")) // STOP
                .append(hex!("5b60016000f3")), // JUMPDEST RETURN(0,1)
        )
        .gas(25)
        .status(StatusCode::Success)
        .gas_left(0)
        .output_data(hex!("00"))
        .check()
}

#[test]
fn jumpi_else() {
    EvmTester::new()
        .code(
            Bytecode::new()
                .opcode(OpCode::COINBASE)
                .opcode(OpCode::DUP1)
                .opcode(OpCode::JUMPI),
        )
        .gas(16)
        .status(StatusCode::Success)
        .gas_used(15)
        .output_data(hex!(""))
        .check()
}

#[test]
fn jumpi_at_the_end() {
    EvmTester::new()
        .code(hex!("5b6001600057"))
        .gas(1000)
        .status(StatusCode::OutOfGas)
        .gas_used(1000)
        .check()
}

#[test]
fn bad_jumpdest() {
    for opcode in [OpCode::JUMP, OpCode::JUMPI] {
        for hex in [hex!("4345"), hex!("4342")] {
            EvmTester::new()
                .code(Bytecode::new().append(hex).opcode(opcode))
                .apply_host_fn(|host, _| {
                    host.tx_context.block_number = 1;
                    host.tx_context.block_gas_limit = 0;
                    host.tx_context.block_timestamp = 0x80000000;
                })
                .status(StatusCode::BadJumpDestination)
                .gas_left(0)
                .check();
        }
    }
}

#[test]
fn jump_to_block_beginning() {
    EvmTester::new()
        .code(Bytecode::new().jumpi(U256::ZERO, OpCode::MSIZE).jump(4))
        .status(StatusCode::BadJumpDestination)
        .check()
}

#[test]
fn jumpi_stack() {
    for input in [&hex!("") as &[u8], &hex!("ee") as &[u8]] {
        EvmTester::new()
            .code(
                Bytecode::new()
                    .pushv(0xde)
                    .jumpi(U256::from(6_u8), OpCode::CALLDATASIZE)
                    .opcode(OpCode::JUMPDEST)
                    .ret_top(),
            )
            .input(input)
            .output_value(0xde)
            .check()
    }
}

#[test]
fn jump_over_jumpdest() {
    // The code contains 2 consecutive JUMPDESTs. The JUMP at the beginning lands on the second one.
    EvmTester::new()
        .code(
            Bytecode::new()
                .pushv(4)
                .opcode(OpCode::JUMP)
                .opcode(OpCode::JUMPDEST)
                .opcode(OpCode::JUMPDEST),
        )
        .status(StatusCode::Success)
        .gas_used(3 + 8 + 1)
        .check()
}

#[test]
fn jump_to_missing_push_data() {
    EvmTester::new()
        .code(
            Bytecode::new()
                .pushv(5)
                .opcode(OpCode::JUMP)
                .opcode(OpCode::PUSH1),
        )
        .status(StatusCode::BadJumpDestination)
        .check()
}
#[test]
fn jump_to_missing_push_data2() {
    EvmTester::new()
        .code(
            Bytecode::new()
                .pushv(6)
                .opcode(OpCode::JUMP)
                .opcode(OpCode::PUSH2)
                .append(hex!("ef")),
        )
        .status(StatusCode::BadJumpDestination)
        .check()
}

#[test]
fn pc_sum() {
    EvmTester::new()
        .code(
            Bytecode::new()
                .opcode(OpCode::PC)
                .opcode(OpCode::PC)
                .opcode(OpCode::PC)
                .opcode(OpCode::PC)
                .opcode(OpCode::ADD)
                .opcode(OpCode::ADD)
                .opcode(OpCode::ADD)
                .ret_top(),
        )
        .status(StatusCode::Success)
        .output_value(6)
        .check()
}

#[test]
fn pc_after_jump_1() {
    EvmTester::new()
        .code(
            Bytecode::new()
                .pushv(3)
                .opcode(OpCode::JUMP)
                .opcode(OpCode::JUMPDEST)
                .opcode(OpCode::PC)
                .ret_top(),
        )
        .status(StatusCode::Success)
        .output_value(4)
        .check()
}

#[test]
fn pc_after_jump_2() {
    for (input, output) in [(&hex!("") as &[u8], 6), (&hex!("ff") as &[u8], 11)] {
        EvmTester::new()
            .code(
                Bytecode::new()
                    .opcode(OpCode::CALLDATASIZE)
                    .pushv(9)
                    .opcode(OpCode::JUMPI)
                    .pushv(12)
                    .opcode(OpCode::PC)
                    .opcode(OpCode::SWAP1)
                    .opcode(OpCode::JUMP)
                    .opcode(OpCode::JUMPDEST)
                    .opcode(OpCode::GAS)
                    .opcode(OpCode::PC)
                    .opcode(OpCode::JUMPDEST)
                    .ret_top(),
            )
            .input(input)
            .status(StatusCode::Success)
            .output_value(output)
            .check()
    }
}

#[test]
fn byte() {
    EvmTester::new()
        .code(
            Bytecode::new()
                .append(hex!("63aabbccdd")) // aabbccdd
                .append(hex!("8060001a")) // DUP 1 BYTE
                .append(hex!("600053")) // m[0] = 00
                .append(hex!("80601c1a")) // DUP 28 BYTE
                .append(hex!("600253")) // m[2] = aa
                .append(hex!("80601f1a")) // DUP 31 BYTE
                .append(hex!("600453")) // m[4] = dd
                .append(hex!("8060201a")) // DUP 32 BYTE
                .append(hex!("600653")) // m[6] = 00
                .append(hex!("60076000f3")), // RETURN(0,7)
        )
        .gas(72)
        .status(StatusCode::Success)
        .gas_left(0)
        .inspect_output(|output| {
            assert_eq!(output.len(), 7);
            assert_eq!(output[0], 0);
            assert_eq!(output[2], 0xaa);
            assert_eq!(output[4], 0xdd);
            assert_eq!(output[6], 0);
        })
        .check()
}

#[test]
fn byte_overflow() {
    EvmTester::new()
        .code(
            Bytecode::new()
                .pushv(0)
                .opcode(OpCode::NOT)
                .pushv(32)
                .opcode(OpCode::BYTE)
                .ret_top(),
        )
        .output_value(0)
        .check();

    EvmTester::new()
        .code(
            Bytecode::new()
                .pushv(0)
                .opcode(OpCode::NOT)
                .pushb(hex!("ffffffffffffffffffffffffffffffffffff"))
                .opcode(OpCode::BYTE)
                .ret_top(),
        )
        .output_value(0)
        .check();
}

#[test]
fn addmod_mulmod() {
    EvmTester::new()
        .code(
            Bytecode::new()
                .append(hex!(
                    "7fcdeb8272fc01d4d50a6ec165d2ea477af19b9b2c198459f59079583b97e88a66"
                ))
                .append(hex!(
                    "7f52e7e7a03b86f534d2e338aa1bb05ba3539cb2f51304cdbce69ce2d422c456ca"
                ))
                .append(hex!(
                    "7fe0f2f0cae05c220260e1724bdc66a0f83810bd1217bd105cb2da11e257c6cdf6"
                ))
                .append(hex!("82828208")) // DUP DUP DUP ADDMOD
                .append(hex!("600052")) // m[0..]
                .append(hex!("82828209")) // DUP DUP DUP MULMOD
                .append(hex!("602052")) // m[32..]
                .append(hex!("60406000f3")), // RETURN(0,64)
        )
        .gas(67)
        .status(StatusCode::Success)
        .gas_left(0)
        .inspect_output(|output| {
            assert_eq!(
                &output[..32],
                hex!("65ef55f81fe142622955e990252cb5209a11d4db113d842408fd9c7ae2a29a5a")
            );
            assert_eq!(
                &output[32..],
                hex!("34e04890131a297202753cae4c72efd508962c9129aed8b08c8e87ab425b7258")
            );
        })
        .check()
}

#[test]
fn divmod() {
    // Div and mod the -1 by the input and return.
    EvmTester::new()
        .code(hex!("600035600160000381810460005281810660205260406000f3"))
        .input(&hex!("0d") as &[u8])
        .status(StatusCode::Success)
        .gas_used(61)
        .inspect_output(|output| {
            assert_eq!(
                &output[..32],
                hex!("0000000000000000000000000000000000000000000000000000000000000013")
            );
            assert_eq!(
                &output[32..],
                hex!("08ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff")
            );
        })
        .check()
}

#[test]
fn div_by_zero() {
    EvmTester::new()
        .code(
            Bytecode::new()
                .pushv(0)
                .opcode(OpCode::DUP1)
                .pushv(0xff)
                .opcode(OpCode::DIV)
                .opcode(OpCode::SDIV)
                .ret_top(),
        )
        .status(StatusCode::Success)
        .gas_used(34)
        .output_value(0)
        .check()
}

#[test]
fn mod_by_zero() {
    EvmTester::new()
        .code(
            Bytecode::new()
                .pushv(0)
                .opcode(OpCode::DUP1)
                .pushv(0xeffe)
                .opcode(OpCode::MOD)
                .opcode(OpCode::SMOD)
                .ret_top(),
        )
        .status(StatusCode::Success)
        .gas_used(34)
        .output_value(0)
        .check()
}

#[test]
fn addmod_mulmod_by_zero() {
    EvmTester::new()
        .code(hex!("6000358080808008091560005260206000f3"))
        .status(StatusCode::Success)
        .gas_used(52)
        .inspect_output(|output| {
            assert_eq!(output.len(), 32);
            assert_eq!(output[31], 1);
        })
        .check();
}

#[test]
fn signextend() {
    EvmTester::new()
        .code(
            Bytecode::new()
                .append(hex!("62017ffe")) // 017ffe
                .append(hex!("8060000b")) // DUP SIGNEXTEND(0)
                .append(hex!("600052")) // m[0..]
                .append(hex!("8060010b")) // DUP SIGNEXTEND(1)
                .append(hex!("602052")) // m[32..]
                .append(hex!("60406000f3")), // RETURN(0,64)
        )
        .gas(49)
        .status(StatusCode::Success)
        .gas_left(0)
        .inspect_output(|output| {
            assert_eq!(
                &output[..32],
                hex!("fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffe")
            );
            assert_eq!(
                &output[32..],
                hex!("0000000000000000000000000000000000000000000000000000000000007ffe")
            );
        })
        .check();
}

#[test]
fn signextend_31() {
    for (code, output) in [
        (
            hex!("61010160000360081c601e0b60005260206000f3"),
            hex!("fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffe"),
        ),
        (
            hex!("61010160000360081c601f0b60005260206000f3"),
            hex!("00fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffe"),
        ),
    ] {
        EvmTester::new()
            .code(code)
            .revision(Revision::Constantinople)
            .status(StatusCode::Success)
            .gas_used(38)
            .output_value(U256::from_be_bytes(output))
            .check();
    }
}

#[test]
fn exp() {
    EvmTester::new()
        .code(
            Bytecode::new()
                .append(hex!("612019")) // 0x2019
                .append(hex!("6003")) // 3
                .append(hex!("0a")) // EXP
                .append(hex!("600052")) // m[0..]
                .append(hex!("60206000f3")), // RETURN(0,32)
        )
        .gas(131)
        .status(StatusCode::Success)
        .gas_left(0)
        .output_data(hex!(
            "263cf24662b24c371a647c1340022619306e431bf3a4298d4b5998a3f1c1aaa3"
        ))
        .check()
}

#[test]
fn exp_1_0() {
    EvmTester::new()
        .code(
            Bytecode::new()
                .pushv(0)
                .pushv(1)
                .opcode(OpCode::EXP)
                .ret_top(),
        )
        .gas(31)
        .status(StatusCode::Success)
        .gas_used(31)
        .output_value(1)
        .check()
}

#[test]
fn exp_0_0() {
    EvmTester::new()
        .code(
            Bytecode::new()
                .pushv(0)
                .pushv(0)
                .opcode(OpCode::EXP)
                .ret_top(),
        )
        .gas(31)
        .status(StatusCode::Success)
        .gas_used(31)
        .output_value(1)
        .check()
}

#[test]
fn exp_oog() {
    let code = hex!("6001600003800a");
    EvmTester::new()
        .code(code)
        .gas(1622)
        .status(StatusCode::Success)
        .gas_left(0)
        .check();
    EvmTester::new()
        .code(code)
        .gas(1621)
        .status(StatusCode::OutOfGas)
        .gas_left(0)
        .check();
}

#[test]
fn exp_pre_spurious_dragon() {
    EvmTester::new()
        .code(
            Bytecode::new()
                .append(hex!("62012019")) // 0x012019
                .append(hex!("6003")) // 3
                .append(hex!("0a")) // EXP
                .append(hex!("600052")) // m[0..]
                .append(hex!("60206000f3")), // RETURN(0,32)
        )
        .revision(Revision::Tangerine)
        .gas(131 - 70)
        .status(StatusCode::Success)
        .gas_left(0)
        .output_data(hex!(
            "422ea3761c4f6517df7f102bb18b96abf4735099209ca21256a6b8ac4d1daaa3"
        ))
        .check();
}

#[test]
fn calldataload() {
    EvmTester::new()
        .code(
            Bytecode::new()
                .append(hex!("600335")) // CALLDATALOAD(3)
                .append(hex!("600052")) // m[0..]
                .append(hex!("600a6000f3")), // RETURN(0,10)
        )
        .gas(21)
        .input(&hex!("0102030405") as &[u8])
        .status(StatusCode::Success)
        .gas_left(0)
        .output_data(hex!("04050000000000000000"))
        .check()
}

#[test]
fn calldataload_outofrange() {
    EvmTester::new()
        .code(
            Bytecode::new()
                .pushv(1)
                .opcode(OpCode::CALLDATALOAD)
                .ret_top(),
        )
        .status(StatusCode::Success)
        .output_value(0)
        .check()
}

#[test]
fn calldatacopy() {
    let code = Bytecode::new()
        .append(hex!("366001600037")) // CALLDATASIZE 1 0 CALLDATACOPY
        .append(hex!("600a6000f3"));
    EvmTester::new()
        .code(code.clone())
        .input(&hex!("0102030405") as &[u8])
        .status(StatusCode::Success)
        .gas_used(23)
        .output_data(hex!("02030405000000000000"))
        .check();

    EvmTester::new()
        .code(code)
        .status(StatusCode::Success)
        .gas_used(20)
        .check();

    EvmTester::new()
        .code(hex!("60ff66fffffffffffffa60003760ff6000f3"))
        .status(StatusCode::Success)
        .gas_used(66)
        .output_data([0; 0xff])
        .check();
}

#[test]
fn address() {
    EvmTester::new()
        .code(
            Bytecode::new()
                .append(hex!("30600052")) // ADDRESS MSTORE(0)
                .append(hex!("600a600af3")), // RETURN(10,10)
        )
        .destination(hex!("cc00000000000000000000000000000000000000"))
        .status(StatusCode::Success)
        .gas(17)
        .gas_left(0)
        .output_data(hex!("0000cc00000000000000"))
        .check()
}

#[test]
fn caller_callvalue() {
    EvmTester::new()
        .code(
            Bytecode::new()
                .append(hex!("333401600052")) // CALLER CALLVALUE ADD MSTORE(0)
                .append(hex!("600a600af3")), // RETURN(10,10)
        )
        .sender(hex!("dd00000000000000000000000000000000000000"))
        .value(U256::from_be_bytes(hex!(
            "00000000000000000000000000ee000000000000000000000000000000000000"
        )))
        .status(StatusCode::Success)
        .gas(22)
        .gas_left(0)
        .output_data(hex!("0000ddee000000000000"))
        .check()
}

#[test]
fn undefined() {
    EvmTester::new()
        .code(hex!("2a"))
        .gas(1)
        .status(StatusCode::UndefinedInstruction)
        .gas_left(0)
        .check()
}

#[test]
fn invalid() {
    EvmTester::new()
        .code(hex!("fe"))
        .gas(1)
        .status(StatusCode::InvalidInstruction)
        .gas_left(0)
        .check()
}

#[test]
fn inner_stop() {
    EvmTester::new()
        .code(
            Bytecode::new()
                .pushv(0)
                .opcode(OpCode::STOP)
                .opcode(OpCode::POP),
        )
        .gas(3)
        .status(StatusCode::Success)
        .gas_used(3)
        .check()
}

#[test]
fn inner_return() {
    EvmTester::new()
        .code(Bytecode::new().ret(0, 0).pushv(0))
        .gas(6)
        .status(StatusCode::Success)
        .gas_used(6)
        .check()
}

#[test]
fn inner_revert() {
    EvmTester::new()
        .code(Bytecode::new().revert(0, 0).pushv(0))
        .gas(6)
        .status(StatusCode::Revert)
        .gas_used(6)
        .check()
}

#[test]
fn inner_invalid() {
    EvmTester::new()
        .revision(Revision::Frontier)
        .code(
            Bytecode::new()
                .pushv(0)
                .append(hex!("fe"))
                .opcode(OpCode::POP),
        )
        .gas(5)
        .status(StatusCode::InvalidInstruction)
        .gas_left(0)
        .check()
}

#[test]
fn inner_selfdestruct() {
    EvmTester::new()
        .revision(Revision::Frontier)
        .code(
            Bytecode::new()
                .pushv(0)
                .opcode(OpCode::SELFDESTRUCT)
                .pushv(0),
        )
        .gas(3)
        .status(StatusCode::Success)
        .gas_used(3)
        .check()
}

#[test]
fn keccak256() {
    EvmTester::new()
        .code(hex!("6108006103ff2060005260206000f3"))
        .status(StatusCode::Success)
        .gas_used(738)
        .output_data(hex!(
            "aeffb38c06e111d84216396baefeb7fed397f303d5cb84a33f1e8b485c4a22da"
        ))
        .check()
}

#[test]
fn keccak256_empty() {
    EvmTester::new()
        .code(
            Bytecode::new()
                .pushv(0)
                .opcode(OpCode::DUP1)
                .opcode(OpCode::KECCAK256)
                .ret_top(),
        )
        .output_data(hex!(
            "c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470"
        ))
        .check()
}

#[test]
fn revert() {
    EvmTester::new()
        .code(
            Bytecode::new()
                .append(hex!("60ee8053")) // m[ee] == e
                .append(hex!("600260edfd")), // REVERT(ee,1)
        )
        .gas_used(39)
        .status(StatusCode::Revert)
        .output_data(hex!("00ee"))
        .check()
}

#[test]
fn return_empty_buffer_at_offset_0() {
    EvmTester::new()
        .code(
            Bytecode::new()
                .opcode(OpCode::MSIZE)
                .opcode(OpCode::DUP1)
                .opcode(OpCode::RETURN),
        )
        .gas_used(5)
        .check()
}

#[test]
fn return_empty_buffer_at_high_offset() {
    for (opcode, status) in [
        (OpCode::RETURN, StatusCode::Success),
        (OpCode::REVERT, StatusCode::Revert),
    ] {
        EvmTester::new()
            .code(
                Bytecode::new()
                    .pushv(0)
                    .opcode(OpCode::DIFFICULTY)
                    .opcode(opcode),
            )
            .apply_host_fn(|host, _| {
                host.tx_context.block_difficulty = U256::from_be_bytes(hex!(
                    "fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff1"
                ))
            })
            .status(status)
            .check();
    }
}

#[test]
fn shl() {
    EvmTester::new()
        .code(hex!("600560011b6000526001601ff3"))
        .revision(Revision::Constantinople)
        .gas_used(24)
        .status(StatusCode::Success)
        .output_data([5 << 1])
        .check()
}

#[test]
fn shr() {
    EvmTester::new()
        .code(hex!("600560011c6000526001601ff3"))
        .revision(Revision::Constantinople)
        .gas_used(24)
        .status(StatusCode::Success)
        .output_data([5 >> 1])
        .check()
}

#[test]
fn sar() {
    EvmTester::new()
        .code(hex!("600160000360021d60005260016000f3"))
        .revision(Revision::Constantinople)
        .gas_used(30)
        .status(StatusCode::Success)
        .output_data([0xff])
        .check() // MSB of (-1 >> 2) == -1
}

#[test]
fn sar_01() {
    EvmTester::new()
        .code(hex!("600060011d60005260016000f3"))
        .revision(Revision::Constantinople)
        .gas_used(24)
        .status(StatusCode::Success)
        .output_data([0])
        .check()
}

#[test]
fn shift_overflow() {
    for op in [OpCode::SHL, OpCode::SHR, OpCode::SAR] {
        EvmTester::new()
            .code(
                Bytecode::new()
                    .pushv(0)
                    .opcode(OpCode::NOT)
                    .pushv(0x100)
                    .opcode(op)
                    .ret_top(),
            )
            .revision(Revision::Constantinople)
            .inspect_output(move |output| {
                assert_eq!(
                    output.iter().copied().map(u64::from).sum::<u64>(),
                    if op == OpCode::SAR { 32 * 0xff } else { 0 }
                );
            })
            .check()
    }
}

#[test]
fn undefined_instruction_analysis_overflow() {
    let undefined_opcode = OpCode(0x0c);
    EvmTester::new()
        .code(Bytecode::new().opcode(undefined_opcode))
        .status(StatusCode::UndefinedInstruction)
        .check()
}

#[test]
fn abort() {
    for r in Revision::iter() {
        EvmTester::new()
            .code(hex!("fe"))
            .revision(r)
            .status(StatusCode::InvalidInstruction)
            .check()
    }
}

#[test]
fn staticmode() {
    for op in [
        OpCode::SSTORE,
        OpCode::LOG0,
        OpCode::LOG1,
        OpCode::LOG2,
        OpCode::LOG3,
        OpCode::LOG4,
        OpCode::CALL,
        OpCode::CREATE,
        OpCode::CREATE2,
        OpCode::SELFDESTRUCT,
    ] {
        let mut code_prefix = Bytecode::new().pushv(1);
        for _ in 0..6 {
            code_prefix = code_prefix.opcode(OpCode::DUP1);
        }

        EvmTester::new()
            .code(code_prefix.opcode(op))
            .revision(Revision::Constantinople)
            .set_static(true)
            .status(StatusCode::StaticModeViolation)
            .gas_left(0)
            .check()
    }
}

#[test]
fn memory_big_allocation() {
    const SIZE: usize = 256 * 1024 + 1;
    EvmTester::new()
        .code(Bytecode::new().ret(0, SIZE))
        .status(StatusCode::Success)
        .output_data([0; SIZE])
        .check()
}

#[test]
fn memory_grow_mstore8() {
    let code = Bytecode::new()
        .pushv(0)
        .opcode(OpCode::CALLDATALOAD)
        .pushv(0)
        .opcode(OpCode::JUMPDEST)
        .opcode(OpCode::DUP1)
        .opcode(OpCode::DUP1)
        .opcode(OpCode::MSTORE8)
        .pushv(1)
        .opcode(OpCode::ADD)
        .opcode(OpCode::DUP1)
        .opcode(OpCode::DUP3)
        .opcode(OpCode::EQ)
        .opcode(OpCode::ISZERO)
        .pushv(5)
        .opcode(OpCode::JUMPI)
        .opcode(OpCode::MSIZE)
        .pushv(0)
        .opcode(OpCode::RETURN);

    const SIZE: usize = 4 * 1024 + 256 + 1;
    let input = hex!("0000000000000000000000000000000000000000000000000000000000001101").to_vec();

    EvmTester::new()
        .code(code)
        .input(input)
        .status(StatusCode::Success)
        .inspect_output(|output| {
            assert_eq!(output.len(), ((SIZE + 31) / 32) * 32);
            for i in 0..SIZE {
                assert_eq!(output[i] as usize, i % 256);
            }

            for i in SIZE..output.len() {
                assert_eq!(output[i], 0);
            }
        })
        .check()
}

#[test]
fn mstore8_memory_cost() {
    for (gas, status) in [(12, StatusCode::Success), (11, StatusCode::OutOfGas)] {
        EvmTester::new()
            .code(Bytecode::new().pushv(0).mstore8(0))
            .gas(gas)
            .status(status)
            .check()
    }
}

#[test]
fn keccak256_memory_cost() {
    for (gas, status) in [(45, StatusCode::Success), (44, StatusCode::OutOfGas)] {
        EvmTester::new()
            .code(Bytecode::new().pushv(1).pushv(0).opcode(OpCode::KECCAK256))
            .gas(gas)
            .status(status)
            .check()
    }
}

#[test]
fn calldatacopy_memory_cost() {
    for (gas, status) in [(18, StatusCode::Success), (17, StatusCode::OutOfGas)] {
        EvmTester::new()
            .code(
                Bytecode::new()
                    .pushv(1)
                    .pushv(0)
                    .pushv(0)
                    .opcode(OpCode::CALLDATACOPY),
            )
            .gas(gas)
            .status(status)
            .check()
    }
}

const MAX_CODE_SIZE: usize = 0x6000;

#[test]
fn max_code_size_push1() {
    let mut code = Bytecode::new();
    for _ in 0..MAX_CODE_SIZE / 2 {
        code = code.pushv(1);
    }
    let code = code.build();
    assert_eq!(code.len(), MAX_CODE_SIZE);

    EvmTester::new()
        .code(code.clone())
        .status(StatusCode::StackOverflow)
        .check();
    EvmTester::new()
        .code(code[..code.len() - 1].to_vec())
        .status(StatusCode::StackOverflow)
        .check();
}

#[test]
fn reverse_16_stack_items() {
    // This test puts values 1, 2, ... , 16 on the stack and then reverse them with SWAP opcodes.
    // This uses all variants of SWAP instruction.

    let n = 16;
    let mut code = Bytecode::new();
    for i in 1..=n {
        code = code.pushv(i);
    }
    code = code.pushv(0); // Temporary stack item.
    code = code
        .opcode(OpCode::SWAP16)
        .opcode(OpCode::SWAP1)
        .opcode(OpCode::SWAP16); // Swap 1 and 16.
    code = code
        .opcode(OpCode::SWAP15)
        .opcode(OpCode::SWAP2)
        .opcode(OpCode::SWAP15); // Swap 2 and 15.
    code = code
        .opcode(OpCode::SWAP14)
        .opcode(OpCode::SWAP3)
        .opcode(OpCode::SWAP14);
    code = code
        .opcode(OpCode::SWAP13)
        .opcode(OpCode::SWAP4)
        .opcode(OpCode::SWAP13);
    code = code
        .opcode(OpCode::SWAP12)
        .opcode(OpCode::SWAP5)
        .opcode(OpCode::SWAP12);
    code = code
        .opcode(OpCode::SWAP11)
        .opcode(OpCode::SWAP6)
        .opcode(OpCode::SWAP11);
    code = code
        .opcode(OpCode::SWAP10)
        .opcode(OpCode::SWAP7)
        .opcode(OpCode::SWAP10);
    code = code
        .opcode(OpCode::SWAP9)
        .opcode(OpCode::SWAP8)
        .opcode(OpCode::SWAP9);
    code = code.opcode(OpCode::POP);
    for i in 0..n {
        code = code.mstore8(i);
    }
    code = code.ret(0, n);

    EvmTester::new()
        .code(code)
        .status(StatusCode::Success)
        .output_data(hex!("0102030405060708090a0b0c0d0e0f10"))
        .check()
}

#[test]
fn memory_access() {
    struct MemoryAccessOpcode {
        opcode: OpCode,
        memory_index_arg: i8,
        memory_size_arg: i8,
    }

    impl From<(OpCode, i8, i8)> for MemoryAccessOpcode {
        fn from((opcode, memory_index_arg, memory_size_arg): (OpCode, i8, i8)) -> Self {
            Self {
                opcode,
                memory_index_arg,
                memory_size_arg,
            }
        }
    }

    struct MemoryAccessParams {
        index: u64,
        size: u64,
    }

    impl From<(u64, u64)> for MemoryAccessParams {
        fn from((index, size): (u64, u64)) -> Self {
            Self { index, size }
        }
    }

    let memory_access_opcodes: Vec<MemoryAccessOpcode> = vec![
        (OpCode::KECCAK256, 0, 1),
        (OpCode::CALLDATACOPY, 0, 2),
        (OpCode::CODECOPY, 0, 2),
        (OpCode::MLOAD, 0, -1),
        (OpCode::MSTORE, 0, -1),
        (OpCode::MSTORE8, 0, -1),
        (OpCode::EXTCODECOPY, 1, 3),
        (OpCode::RETURNDATACOPY, 0, 2),
        (OpCode::LOG0, 0, 1),
        (OpCode::LOG1, 0, 1),
        (OpCode::LOG2, 0, 1),
        (OpCode::LOG3, 0, 1),
        (OpCode::LOG4, 0, 1),
        (OpCode::RETURN, 0, 1),
        (OpCode::REVERT, 0, 1),
        (OpCode::CALL, 3, 4),
        (OpCode::CALL, 5, 6),
        (OpCode::CALLCODE, 3, 4),
        (OpCode::CALLCODE, 5, 6),
        (OpCode::DELEGATECALL, 2, 3),
        (OpCode::DELEGATECALL, 4, 5),
        (OpCode::STATICCALL, 2, 3),
        (OpCode::STATICCALL, 4, 5),
        (OpCode::CREATE, 1, 2),
        (OpCode::CREATE2, 1, 2),
    ]
    .into_iter()
    .map(MemoryAccessOpcode::from)
    .collect();

    let memory_access_test_cases: Vec<MemoryAccessParams> = vec![
        (0, 0x100000000),
        (0x80000000, 0x80000000),
        (0x100000000, 0),
        (0x100000000, 1),
        (0x100000000, 0x100000000),
    ]
    .into_iter()
    .map(MemoryAccessParams::from)
    .collect();

    let metrics = &*crate::execution::evm::instructions::PROPERTIES;

    for p in memory_access_test_cases {
        let push_size = format!("64{:0>10x}", p.size);
        let push_index = format!("64{:0>10x}", p.index);

        for t in &memory_access_opcodes {
            let num_args = metrics[t.opcode.to_usize()].unwrap().stack_height_required as i8;
            let mut h = max(num_args, t.memory_size_arg + 1);
            let mut code = Bytecode::new();

            if t.memory_size_arg >= 0 {
                h -= 1;
                while h != t.memory_size_arg {
                    code = code.pushv(0);
                    h -= 1;
                }

                code = code.append(hex::decode(&push_size).unwrap());
            } else if p.index == 0 || p.size == 0 {
                continue; // Skip opcodes not having SIZE argument.
            }

            h -= 1;
            while h != t.memory_index_arg {
                code = code.pushv(0);
                h -= 1;
            }

            code = code.append(hex::decode(&push_index).unwrap());

            while h != 0 {
                code = code.pushv(0);
                h -= 1;
            }

            code = code.opcode(t.opcode);

            let gas = 8796294610952;

            println!(
                "offset = {:#02x} size = {:#02x} opcode {}",
                p.index, p.size, t.opcode
            );

            let tester = EvmTester::new()
                .code(code)
                .gas(gas)
                .revision(Revision::Constantinople);

            if p.size == 0 {
                // It is allowed to request 0 size memory at very big offset.
                assert_ne!(
                    tester
                        .status(if t.opcode == OpCode::REVERT {
                            StatusCode::Revert
                        } else {
                            StatusCode::Success
                        })
                        .check_and_get_result()
                        .gas_left,
                    0
                );
            } else {
                if t.opcode == OpCode::RETURNDATACOPY {
                    // In case of RETURNDATACOPY the "invalid memory access" might also be returned.
                    tester.status_one_of([StatusCode::OutOfGas, StatusCode::InvalidMemoryAccess])
                } else {
                    tester.status(StatusCode::OutOfGas)
                }
                .gas_left(0)
                .check();
            }
        }
    }
}
