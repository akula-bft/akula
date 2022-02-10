use crate::{
    execution::evm::{opcode::*, util::*, *},
    models::*,
};

#[test]
fn basefee_pre_london() {
    EvmTester::new()
        .revision(Revision::Berlin)
        .code(Bytecode::new().opcode(OpCode::BASEFEE))
        .status(StatusCode::UndefinedInstruction)
        .check()
}

#[test]
fn basefee_nominal_case() {
    // https://eips.ethereum.org/EIPS/eip-3198#nominal-case
    let t = EvmTester::new()
        .revision(Revision::London)
        .apply_host_fn(|host, _| {
            host.tx_context.block_base_fee = 7_u128.into();
        });
    t.clone()
        .code(Bytecode::new().opcode(OpCode::BASEFEE).opcode(OpCode::STOP))
        .status(StatusCode::Success)
        .gas_used(2)
        .check();

    t.code(Bytecode::new().opcode(OpCode::BASEFEE).ret_top())
        .status(StatusCode::Success)
        .gas_used(17)
        .output_value(7_u128)
        .check()
}
