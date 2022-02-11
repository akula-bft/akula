use crate::{
    execution::evm::{host::*, opcode::*, util::*, *},
    models::*,
};
use hex_literal::hex;

#[test]
fn eip2929_case1() {
    // https://gist.github.com/holiman/174548cad102096858583c6fbbb0649a#case-1
    EvmTester::new()
        .revision(Revision::Berlin)
        .sender(hex!("0000000000000000000000000000000000000000"))
        .destination(hex!("000000000000000000000000636F6E7472616374"))
        .gas(13653)
        .code(hex!("60013f5060023b506003315060f13f5060f23b5060f3315060f23f5060f33b5060f1315032315030315000"))
        .status(StatusCode::Success)
        .gas_used(8653)
        .output_data([])
        .inspect_host(|host, msg| {
            assert_eq!(
                host.recorded.account_accesses,
                [
                    msg.sender,
                    msg.recipient,
                    hex!("0000000000000000000000000000000000000001").into(),
                    hex!("0000000000000000000000000000000000000001").into(),
                    hex!("0000000000000000000000000000000000000002").into(),
                    hex!("0000000000000000000000000000000000000002").into(),
                    hex!("0000000000000000000000000000000000000003").into(),
                    hex!("0000000000000000000000000000000000000003").into(),
                    hex!("00000000000000000000000000000000000000f1").into(),
                    hex!("00000000000000000000000000000000000000f1").into(),
                    hex!("00000000000000000000000000000000000000f2").into(),
                    hex!("00000000000000000000000000000000000000f2").into(),
                    hex!("00000000000000000000000000000000000000f3").into(),
                    hex!("00000000000000000000000000000000000000f3").into(),
                    hex!("00000000000000000000000000000000000000f2").into(),
                    hex!("00000000000000000000000000000000000000f2").into(),
                    hex!("00000000000000000000000000000000000000f3").into(),
                    hex!("00000000000000000000000000000000000000f3").into(),
                    hex!("00000000000000000000000000000000000000f1").into(),
                    hex!("00000000000000000000000000000000000000f1").into(),
                    hex!("0000000000000000000000000000000000000000").into(),
                    hex!("0000000000000000000000000000000000000000").into(),
                    msg.recipient,
                    msg.recipient,
                ]
            );
        })
        .check()
}

#[test]
fn eip2929_case2() {
    // https://gist.github.com/holiman/174548cad102096858583c6fbbb0649a#case-2
    EvmTester::new()
        .revision(Revision::Berlin)
        .sender(hex!("0000000000000000000000000000000000000000"))
        .destination(hex!("000000000000000000000000636F6E7472616374"))
        .code(hex!(
            "60006000600060ff3c60006000600060ff3c600060006000303c00"
        ))
        .status(StatusCode::Success)
        .gas_used(2835)
        .output_data([])
        .inspect_host(|host, msg| {
            assert_eq!(
                host.recorded.account_accesses,
                [
                    msg.sender,
                    msg.recipient,
                    hex!("00000000000000000000000000000000000000ff").into(),
                    hex!("00000000000000000000000000000000000000ff").into(),
                    msg.recipient,
                ]
            );
        })
        .check()
}

#[test]
fn eip2929_case3() {
    // https://gist.github.com/holiman/174548cad102096858583c6fbbb0649a#case-3
    EvmTester::new()
        .revision(Revision::Berlin)
        .sender(hex!("0000000000000000000000000000000000000000"))
        .destination(hex!("000000000000000000000000636F6E7472616374"))
        .code(hex!("60015450601160015560116002556011600255600254600154"))
        .status(StatusCode::Success)
        .gas_used(44529)
        .output_data([])
        .check()
}

#[test]
fn eip2929_case4() {
    // https://gist.github.com/holiman/174548cad102096858583c6fbbb0649a#case-4
    EvmTester::new()
        .revision(Revision::Berlin)
        .sender(hex!("0000000000000000000000000000000000000000"))
        .destination(hex!("000000000000000000000000636F6E7472616374"))
        .code(hex!(
            "60008080808060046000f15060008080808060ff6000f15060008080808060ff6000fa50"
        ))
        .status(StatusCode::Success)
        .gas_used(2869)
        .output_data([])
        .check()
}

#[test]
fn eip2929_op_oog() {
    for (op, gas) in [
        (OpCode::BALANCE, 2603),
        (OpCode::EXTCODESIZE, 2603),
        (OpCode::EXTCODEHASH, 2603),
    ] {
        let t = EvmTester::new()
            .revision(Revision::Berlin)
            .code(Bytecode::new().pushv(0x0a).opcode(op));

        t.clone()
            .gas(gas)
            .status(StatusCode::Success)
            .gas_used(gas)
            .check();

        t.clone()
            .gas(gas - 1)
            .status(StatusCode::OutOfGas)
            .gas_used(gas - 1)
            .check();
    }
}

#[test]
fn eip2929_extcodecopy_oog() {
    let t = EvmTester::new().revision(Revision::Berlin).code(
        Bytecode::new()
            .pushv(0)
            .opcode(OpCode::DUP1)
            .opcode(OpCode::DUP1)
            .pushv(0xa)
            .opcode(OpCode::EXTCODECOPY),
    );

    t.clone()
        .gas(2612)
        .status(StatusCode::Success)
        .gas_used(2612)
        .check();

    t.gas(2611)
        .status(StatusCode::OutOfGas)
        .gas_used(2611)
        .check();
}

#[test]
fn eip2929_sload_cold() {
    let key = 1_u8.into();

    let t = EvmTester::new()
        .revision(Revision::Berlin)
        .code(Bytecode::new().pushv(1).opcode(OpCode::SLOAD))
        .apply_host_fn(move |host, msg| {
            let mut st = host
                .accounts
                .entry(msg.recipient)
                .or_default()
                .storage
                .entry(key)
                .or_default();
            st.value = 2_u8.into();
            assert_eq!(st.access_status, AccessStatus::Cold);
        });

    t.clone()
        .gas(2103)
        .status(StatusCode::Success)
        .gas_used(2103)
        .inspect_host(move |host, msg| {
            assert_eq!(
                host.accounts[&msg.recipient].storage[&key].access_status,
                AccessStatus::Warm
            );
        })
        .check();

    t.gas(2102)
        .status(StatusCode::OutOfGas)
        .gas_used(2102)
        .check();
}

#[test]
fn eip2929_sload_two_slots() {
    let key0 = 0_u8.into();
    let key1 = 1_u8.into();

    EvmTester::new()
        .revision(Revision::Berlin)
        .code(
            Bytecode::new()
                .pushv(key0)
                .opcode(OpCode::SLOAD)
                .opcode(OpCode::POP)
                .pushv(key1)
                .opcode(OpCode::SLOAD)
                .opcode(OpCode::POP),
        )
        .gas(30000)
        .status(StatusCode::Success)
        .gas_used(4210)
        .inspect_host(move |host, msg| {
            assert_eq!(
                host.accounts[&msg.recipient].storage[&key0].access_status,
                AccessStatus::Warm
            );
            assert_eq!(
                host.accounts[&msg.recipient].storage[&key1].access_status,
                AccessStatus::Warm
            );
        })
        .check()
}

#[test]
fn eip2929_sload_warm() {
    let key = 1_u8.into();
    let t = EvmTester::new()
        .revision(Revision::Berlin)
        .code(Bytecode::new().pushv(1).opcode(OpCode::SLOAD))
        .apply_host_fn(move |host, msg| {
            let st = host
                .accounts
                .entry(msg.recipient)
                .or_default()
                .storage
                .entry(key)
                .or_default();
            st.value = 2_u8.into();
            st.access_status = AccessStatus::Warm;
        });

    t.clone()
        .gas(103)
        .status(StatusCode::Success)
        .gas_used(103)
        .inspect_host(move |host, msg| {
            assert_eq!(
                host.accounts[&msg.recipient].storage[&key].access_status,
                AccessStatus::Warm
            );
        })
        .check();

    t.gas(102)
        .status(StatusCode::OutOfGas)
        .gas_used(102)
        .check();
}

#[test]
fn eip2929_sstore_modify_cold() {
    let key = 1_u8.into();
    let t = EvmTester::new()
        .revision(Revision::Berlin)
        .code(Bytecode::new().sstore(1, 3))
        .apply_host_fn(move |host, msg| {
            host.accounts
                .entry(msg.recipient)
                .or_default()
                .storage
                .entry(key)
                .or_default()
                .value = 2_u8.into();
        });

    t.clone()
        .gas(5006)
        .status(StatusCode::Success)
        .gas_used(5006)
        .inspect_host(move |host, msg| {
            assert_eq!(host.accounts[&msg.recipient].storage[&key].value, 3);
            assert_eq!(
                host.accounts[&msg.recipient].storage[&key].access_status,
                AccessStatus::Warm
            );
        })
        .check();

    t.gas(5005)
        .status(StatusCode::OutOfGas)
        .gas_used(5005)
        .inspect_host(move |host, msg| {
            // The storage will be modified anyway, because the cost is checked after.
            assert_eq!(host.accounts[&msg.recipient].storage[&key].value, 3);
            assert_eq!(
                host.accounts[&msg.recipient].storage[&key].access_status,
                AccessStatus::Warm
            );
        })
        .check();
}

#[test]
fn eip2929_selfdestruct_cold_beneficiary() {
    let t = EvmTester::new()
        .revision(Revision::Berlin)
        .code(Bytecode::new().pushv(0xbe).opcode(OpCode::SELFDESTRUCT));

    t.clone()
        .gas(7603)
        .status(StatusCode::Success)
        .gas_used(7603)
        .check();

    t.gas(7602)
        .status(StatusCode::OutOfGas)
        .gas_used(7602)
        .check();
}

#[test]
fn eip2929_selfdestruct_warm_beneficiary() {
    let t = EvmTester::new()
        .revision(Revision::Berlin)
        .code(Bytecode::new().pushv(0xbe).opcode(OpCode::SELFDESTRUCT))
        .apply_host_fn(|host, _| {
            host.access_account(hex!("00000000000000000000000000000000000000be").into());
        });

    t.clone()
        .gas(5003)
        .status(StatusCode::Success)
        .gas_used(5003)
        .check();

    t.gas(5002)
        .status(StatusCode::OutOfGas)
        .gas_used(5002)
        .check();
}

#[test]
fn eip2929_delegatecall_cold() {
    let t = EvmTester::new()
        .revision(Revision::Berlin)
        .code(CallInstruction::delegatecall(0xde));

    t.clone()
        .gas(2618)
        .status(StatusCode::Success)
        .gas_used(2618)
        .inspect_host(|host, msg| {
            assert_eq!(
                host.recorded.account_accesses,
                [
                    msg.sender,
                    msg.recipient,
                    hex!("00000000000000000000000000000000000000de").into(),
                    msg.sender,
                ]
            );
        })
        .check();

    t.gas(2617)
        .status(StatusCode::OutOfGas)
        .gas_used(2617)
        .inspect_host(|host, msg| {
            assert_eq!(
                host.recorded.account_accesses,
                [
                    msg.sender,
                    msg.recipient,
                    hex!("00000000000000000000000000000000000000de").into(),
                ]
            );
        })
        .check();
}
