use crate::execution::evm::{opcode::*, util::*, *};

#[test]
fn loop_full_of_jumpdests() {
    // The code is a simple loop with a counter taken from the input or a constant (325) if the
    // input is zero. The loop body contains of only JUMPDESTs, as much as the code size limit
    // allows.

    // The `mul(325, iszero(dup1(calldataload(0)))) + OP_OR` is equivalent of
    // `((x == 0) * 325) | x`
    // what is
    // `x == 0 ? 325 : x`.

    // The `not_(0)` is -1 so we can do `loop_counter + (-1)` to decrease the loop counter.

    let code = Bytecode::new()
        .pushv(15)
        .pushv(0_u128)
        .opcode(OpCode::NOT)
        .pushv(0_u128)
        .opcode(OpCode::CALLDATALOAD)
        .opcode(OpCode::DUP1)
        .opcode(OpCode::ISZERO)
        .pushv(325)
        .opcode(OpCode::MUL)
        + OpCode::OR
        + (MAX_CODE_SIZE - 20) * OpCode::JUMPDEST
        + OpCode::DUP2
        + OpCode::ADD
        + OpCode::DUP1
        + OpCode::DUP4
        + OpCode::JUMPI;

    assert_eq!(code.clone().build().len(), MAX_CODE_SIZE);

    EvmTester::new()
        .code(code)
        .status(StatusCode::Success)
        .gas_used(7987882)
        .check()
}

#[test]
fn jumpdest_with_high_offset() {
    for offset in [3, 16383, 16384, 32767, 32768, 65535, 65536] {
        let mut code = Bytecode::new().pushv(offset).opcode(OpCode::JUMP).build();
        code.resize(offset, OpCode::INVALID.to_u8());
        code.push(OpCode::JUMPDEST.to_u8());
        EvmTester::new()
            .code(code)
            .status(StatusCode::Success)
            .check()
    }
}
