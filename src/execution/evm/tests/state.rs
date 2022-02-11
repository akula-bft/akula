use crate::{
    execution::evm::{
        opcode::*,
        util::{mocked_host::*, *},
        *,
    },
    models::*,
};
use ethereum_types::*;
use ethnum::{AsU256, U256};
use hex_literal::hex;

#[test]
fn code() {
    // CODESIZE 2 0 CODECOPY RETURN(0,9)
    let code = hex!("38600260003960096000f3");
    EvmTester::new()
        .code(code)
        .gas_used(23)
        .output_data(&code[2..11])
        .check()
}

#[test]
fn codecopy_combinations() {
    // The CODECOPY arguments are provided in calldata: first byte is index, second byte is size.
    // The whole copied code is returned.
    let code = Bytecode::new()
        .pushv(0_u128)
        .opcode(OpCode::CALLDATALOAD)
        .pushv(1_u128)
        .opcode(OpCode::BYTE)
        .opcode(OpCode::DUP1)
        .pushv(0_u128)
        .opcode(OpCode::CALLDATALOAD)
        .pushv(0_u128)
        .opcode(OpCode::BYTE)
        .pushv(0_u128)
        .opcode(OpCode::CODECOPY)
        .pushv(0_u128)
        .opcode(OpCode::RETURN)
        .build();

    assert_eq!(code.len(), 0x13);

    for (input, output) in [
        (hex!("0013"), code.clone()),
        (hex!("0012"), code[..0x12].to_vec()),
        (hex!("0014"), code.iter().copied().chain([0]).collect()),
        (hex!("1300"), vec![]),
        (hex!("1400"), vec![]),
        (hex!("1200"), vec![]),
        (hex!("1301"), hex!("00").to_vec()),
        (hex!("1401"), hex!("00").to_vec()),
        (hex!("1201"), code[0x12..0x12 + 1].to_vec()),
    ] {
        EvmTester::new()
            .code(code.clone())
            .input(input.to_vec())
            .output_data(output)
            .check()
    }
}

#[test]
fn storage() {
    EvmTester::new()
        .code(
            Bytecode::new()
                .sstore(0xee, 0xff)
                .sload(0xee)
                .mstore8(0)
                .ret(0, 1),
        )
        .gas(100000)
        .status(StatusCode::Success)
        .gas_left(99776 - 20000)
        .output_data(hex!("ff"))
        .check()
}

#[test]
fn sstore_pop_stack() {
    EvmTester::new()
        .code(hex!("60008060015560005360016000f3"))
        .gas(100000)
        .status(StatusCode::Success)
        .output_data(hex!("00"))
        .check()
}

#[test]
fn sload_cost_pre_tangerine_whistle() {
    EvmTester::new()
        .code(hex!("60008054"))
        .revision(Revision::Homestead)
        .apply_host_fn(|host, message| {
            host.accounts.entry(message.recipient).or_default();
        })
        .gas(56)
        .status(StatusCode::Success)
        .gas_left(0)
        .inspect_host(|host, message| {
            assert_eq!(host.accounts[&message.recipient].storage.len(), 0);
        })
        .check()
}

#[test]
fn sstore_out_of_block_gas() {
    for (gas, status) in [
        // Barely enough gas to execute successfully.
        (20011, StatusCode::Success),
        // Out of block gas - 1 too low.
        (20010, StatusCode::OutOfGas),
        // Out of block gas - 2 too low.
        (20009, StatusCode::OutOfGas),
        // SSTORE instructions out of gas.
        (20008, StatusCode::OutOfGas),
    ] {
        EvmTester::new()
            .code(
                Bytecode::new()
                    .pushv(0_u128)
                    .sstore(0, 1)
                    .opcode(OpCode::POP),
            )
            .gas(gas)
            .status(status)
            .check()
    }
}

#[test]
fn sstore_cost() {
    for revision in [
        Revision::Byzantium,
        Revision::Constantinople,
        Revision::Petersburg,
        Revision::Istanbul,
    ] {
        let v1 = 1_u8.into();

        fn get_storage<K: AsU256>(host: &mut MockedHost, key: K) -> &mut StorageValue {
            host.accounts
                .entry(Address::zero())
                .or_default()
                .storage
                .entry(key.as_u256())
                .or_default()
        }

        let t = EvmTester::new().revision(revision);

        // Added:
        t.clone()
            .code(Bytecode::new().sstore(1, 1))
            .gas_used(20006)
            .status(StatusCode::Success)
            .check();

        // Deleted:
        t.clone()
            .code(Bytecode::new().sstore(1, 0))
            .apply_host_fn(move |host, _| {
                get_storage(host, v1).value = v1;
            })
            .gas_used(5006)
            .status(StatusCode::Success)
            .check();

        // Modified:
        t.clone()
            .code(Bytecode::new().sstore(1, 2))
            .apply_host_fn(move |host, _| {
                get_storage(host, v1).value = v1;
            })
            .gas_used(5006)
            .status(StatusCode::Success)
            .check();

        // Unchanged:
        t.clone()
            .code(Bytecode::new().sstore(1, 1))
            .apply_host_fn(move |host, _| {
                get_storage(host, v1).value = v1;
            })
            .gas_used(match revision {
                Revision::Istanbul => 806,
                Revision::Constantinople => 206,
                _ => 5006,
            })
            .status(StatusCode::Success)
            .check();

        // Added & unchanged:
        t.clone()
            .code(Bytecode::new().sstore(1, 1).sstore(1, 1))
            .gas_used(match revision {
                Revision::Istanbul => 20812,
                Revision::Constantinople => 20212,
                _ => 25012,
            })
            .status(StatusCode::Success)
            .check();

        // Modified again:
        t.clone()
            .code(Bytecode::new().sstore(1, 2))
            .apply_host_fn(move |host, _| {
                let s = get_storage(host, v1);
                s.dirty = true;
                s.value = v1;
            })
            .status(StatusCode::Success)
            .gas_used(match revision {
                Revision::Istanbul => 806,
                Revision::Constantinople => 206,
                _ => 5006,
            })
            .check();

        // Added & modified again:
        t.clone()
            .code(Bytecode::new().sstore(1, 1).sstore(1, 2))
            .status(StatusCode::Success)
            .gas_used(match revision {
                Revision::Istanbul => 20812,
                Revision::Constantinople => 20212,
                _ => 25012,
            })
            .check();

        // Modified & modified again:
        t.clone()
            .code(Bytecode::new().sstore(1, 2).sstore(1, 3))
            .apply_host_fn(move |host, _| {
                get_storage(host, v1).value = v1;
            })
            .status(StatusCode::Success)
            .gas_used(match revision {
                Revision::Istanbul => 5812,
                Revision::Constantinople => 5212,
                _ => 10012,
            })
            .check();

        // Modified & modified again back to original:t.clone()
        t.clone()
            .code(Bytecode::new().sstore(1, 2).sstore(1, 1))
            .apply_host_fn(move |host, _| {
                get_storage(host, v1).value = v1;
            })
            .status(StatusCode::Success)
            .gas_used(match revision {
                Revision::Istanbul => 5812,
                Revision::Constantinople => 5212,
                _ => 10012,
            })
            .check();
    }
}

#[test]
fn sstore_below_stipend() {
    let code = Bytecode::new().sstore(0, 0);

    let t = EvmTester::new().code(code);

    for (revision, status) in [
        (Revision::Homestead, StatusCode::OutOfGas),
        (Revision::Constantinople, StatusCode::Success),
        (Revision::Istanbul, StatusCode::OutOfGas),
    ] {
        t.clone()
            .revision(revision)
            .gas(2306)
            .status(status)
            .check()
    }

    t.revision(Revision::Constantinople)
        .gas(2307)
        .status(StatusCode::Success)
        .check()
}

#[test]
fn tx_context() {
    EvmTester::new()
        .code(
            Bytecode::new()
                .opcode(OpCode::TIMESTAMP)
                .opcode(OpCode::COINBASE)
                .opcode(OpCode::OR)
                .opcode(OpCode::GASPRICE)
                .opcode(OpCode::OR)
                .opcode(OpCode::NUMBER)
                .opcode(OpCode::OR)
                .opcode(OpCode::DIFFICULTY)
                .opcode(OpCode::OR)
                .opcode(OpCode::GASLIMIT)
                .opcode(OpCode::OR)
                .opcode(OpCode::ORIGIN)
                .opcode(OpCode::OR)
                .opcode(OpCode::CHAINID)
                .opcode(OpCode::OR)
                .ret_top(),
        )
        .revision(Revision::Istanbul)
        .apply_host_fn(|host, _| {
            host.tx_context.block_timestamp = 0xdd;
            host.tx_context.block_number = 0x1100;
            host.tx_context.block_gas_limit = 0x990000;
            host.tx_context.chain_id = U256::from_be_bytes(hex!(
                "00000000000000000000000000000000000000000000000000000000aa000000"
            ));
            host.tx_context.block_coinbase.0[1] = 0xcc;
            host.tx_context.tx_origin.0[2] = 0x55;
            host.tx_context.block_difficulty = U256::from_be_bytes(hex!(
                "00dd000000000000000000000000000000000000000000000000000000000000"
            ));
            host.tx_context.tx_gas_price = U256::from_be_bytes(hex!(
                "0000660000000000000000000000000000000000000000000000000000000000"
            ));
        })
        .status(StatusCode::Success)
        .gas_used(52)
        .inspect_output(|output_data| {
            assert_eq!(output_data.len(), 32);
            assert_eq!(output_data[31], 0xdd);
            assert_eq!(output_data[30], 0x11);
            assert_eq!(output_data[29], 0x99);
            assert_eq!(output_data[28], 0xaa);
            assert_eq!(output_data[14], 0x55);
            assert_eq!(output_data[13], 0xcc);
            assert_eq!(output_data[2], 0x66);
            assert_eq!(output_data[1], 0xdd);
        })
        .check()
}

#[test]
fn balance() {
    EvmTester::new()
        .apply_host_fn(|host, msg| {
            host.accounts.entry(msg.recipient).or_default().balance = 0x0504030201_u64.into()
        })
        .code(
            Bytecode::new()
                .opcode(OpCode::ADDRESS)
                .opcode(OpCode::BALANCE)
                .mstore(0)
                .ret(32 - 6, 6),
        )
        .gas_used(417)
        .status(StatusCode::Success)
        .output_data(hex!("000504030201"))
        .check()
}

#[test]
fn account_info_homestead() {
    let t = EvmTester::new()
        .revision(Revision::Homestead)
        .apply_host_fn(|host, msg| {
            let acc = host.accounts.entry(msg.recipient).or_default();
            acc.balance = U256::ONE;
            acc.code = [1].to_vec().into();
        });

    t.clone()
        .code(
            Bytecode::new()
                .opcode(OpCode::ADDRESS)
                .opcode(OpCode::BALANCE)
                .ret_top(),
        )
        .status(StatusCode::Success)
        .gas_used(37)
        .output_value(1)
        .check();

    t.clone()
        .code(
            Bytecode::new()
                .opcode(OpCode::ADDRESS)
                .opcode(OpCode::EXTCODESIZE)
                .ret_top(),
        )
        .status(StatusCode::Success)
        .gas_used(37)
        .output_value(1)
        .check();

    t.code(
        Bytecode::new()
            .pushv(1)
            .pushv(0)
            .pushv(0)
            .opcode(OpCode::ADDRESS)
            .opcode(OpCode::EXTCODECOPY)
            .ret(0, 1),
    )
    .status(StatusCode::Success)
    .gas_used(43)
    .output_data([1])
    .check()
}

#[test]
fn selfbalance() {
    let t = EvmTester::new()
        .apply_host_fn(|host, msg| {
            host.accounts.entry(msg.recipient).or_default().balance = 0x0504030201_u64.into();
        })
        // NOTE: adding push here to balance out the stack pre-Istanbul (needed to get undefined
        // instruction as a result)
        .code(
            Bytecode::new()
                .pushv(1)
                .opcode(OpCode::SELFBALANCE)
                .mstore(0)
                .ret(32 - 6, 6),
        );

    t.clone()
        .revision(Revision::Constantinople)
        .status(StatusCode::UndefinedInstruction)
        .check();

    t.revision(Revision::Istanbul)
        .status(StatusCode::Success)
        .gas_used(23)
        .output_data(hex!("000504030201"))
        .check()
}

#[test]
fn log() {
    for op in [
        OpCode::LOG0,
        OpCode::LOG1,
        OpCode::LOG2,
        OpCode::LOG3,
        OpCode::LOG4,
    ] {
        let n = op.to_usize() - OpCode::LOG0.to_usize();
        EvmTester::new()
            .code(
                Bytecode::new()
                    .pushv(1)
                    .pushv(2)
                    .pushv(3)
                    .pushv(4)
                    .mstore8_value(2, 0x77)
                    .pushv(2)
                    .pushv(2)
                    .opcode(op),
            )
            .status(StatusCode::Success)
            .gas_used((421 + n * 375) as i64)
            .inspect_host(move |host, _| {
                assert_eq!(host.recorded.logs.len(), 1);
                let last_log = host.recorded.logs.last().unwrap();
                assert_eq!(&*last_log.data, &hex!("7700") as &[u8]);
                assert_eq!(last_log.topics.len(), n);
                for i in 0..n {
                    assert_eq!(last_log.topics[i], 4 - i as u128);
                }
            })
            .check()
    }
}

#[test]
fn log0_empty() {
    EvmTester::new()
        .code(
            Bytecode::new()
                .pushv(0_u128)
                .opcode(OpCode::DUP1)
                .opcode(OpCode::LOG0),
        )
        .inspect_host(|host, _| {
            assert_eq!(host.recorded.logs.len(), 1);
            let last_log = host.recorded.logs.last().unwrap();
            assert_eq!(last_log.topics.len(), 0);
            assert_eq!(last_log.data.len(), 0);
        })
        .check()
}

#[test]
fn log_data_cost() {
    for op in [
        OpCode::LOG0,
        OpCode::LOG1,
        OpCode::LOG2,
        OpCode::LOG3,
        OpCode::LOG4,
    ] {
        let num_topics = op.to_u8() - OpCode::LOG0.to_u8();
        let mut code = Bytecode::new().pushv(0);
        for _ in 0..4 {
            code = code.opcode(OpCode::DUP1);
        }
        code = code.pushv(1).pushv(0).opcode(op);

        let cost = 407 + num_topics as usize * 375;

        EvmTester::new()
            .code(code)
            .gas_used(cost as i64)
            .status(StatusCode::Success)
            .inspect_host(|host, _| {
                assert_eq!(host.recorded.logs.len(), 1);
            })
            .check()
    }
}

#[test]
fn selfdestruct() {
    EvmTester::new()
        .code(hex!("6009ff"))
        .revision(Revision::Spurious)
        .status(StatusCode::Success)
        .gas_used(5003)
        .inspect_host(|host, _| {
            assert_eq!(
                host.recorded.selfdestructs,
                [SelfdestructRecord {
                    selfdestructed: Address::zero(),
                    beneficiary: Address::from(hex!("0000000000000000000000000000000000000009"))
                }]
            );
        })
        .check();

    EvmTester::new()
        .code(hex!("6007ff"))
        .revision(Revision::Homestead)
        .status(StatusCode::Success)
        .gas_used(3)
        .inspect_host(|host, _| {
            assert_eq!(
                host.recorded.selfdestructs,
                [SelfdestructRecord {
                    selfdestructed: Address::zero(),
                    beneficiary: Address::from(hex!("0000000000000000000000000000000000000007"))
                }]
            );
        })
        .check();

    EvmTester::new()
        .code(hex!("6008ff"))
        .revision(Revision::Tangerine)
        .status(StatusCode::Success)
        .gas_used(30003)
        .inspect_host(|host, _| {
            assert_eq!(
                host.recorded.selfdestructs,
                [SelfdestructRecord {
                    selfdestructed: Address::zero(),
                    beneficiary: Address::from(hex!("0000000000000000000000000000000000000008"))
                }]
            );
        })
        .check();
}

#[test]
fn selfdestruct_with_balance() {
    let beneficiary = Address::zero();
    let code = Bytecode::new()
        .pushb(beneficiary.0)
        .opcode(OpCode::SELFDESTRUCT);

    let mut t = EvmTester::new()
        .code(code)
        .destination(hex!("000000000000000000000000000000000000005e"))
        .apply_host_fn(|host, msg| {
            host.accounts.entry(msg.recipient).or_default().balance = U256::ZERO;
        });

    t.clone()
        .revision(Revision::Homestead)
        .status(StatusCode::Success)
        .gas_used(3)
        .inspect_host(|host, msg| {
            assert_eq!(host.recorded.account_accesses, [msg.recipient]); // Selfdestruct.
        })
        .check();

    t.clone()
        .revision(Revision::Tangerine)
        .status(StatusCode::Success)
        .gas_used(30003)
        .inspect_host(move |host, msg| {
            assert_eq!(
                host.recorded.account_accesses,
                [
                    // Exists?
                    beneficiary,
                    // Selfdestruct.
                    msg.recipient
                ]
            );
        })
        .check();

    t.clone()
        .revision(Revision::Tangerine)
        .gas(30002)
        .status(StatusCode::OutOfGas)
        .inspect_host(move |host, _| {
            assert_eq!(
                host.recorded.account_accesses,
                [
                    // Exists?
                    beneficiary
                ]
            );
        })
        .check();

    t.clone()
        .revision(Revision::Spurious)
        .status(StatusCode::Success)
        .gas_used(5003)
        .inspect_host(move |host, msg| {
            assert_eq!(
                host.recorded.account_accesses,
                [
                    // Balance.
                    msg.recipient,
                    // Selfdestruct.
                    msg.recipient,
                ]
            )
        })
        .check();

    t.clone()
        .revision(Revision::Spurious)
        .gas(5002)
        .status(StatusCode::OutOfGas)
        .inspect_host(|host, _| {
            assert_eq!(host.recorded.account_accesses, []);
        })
        .check();

    t = t.apply_host_fn(move |host, msg| {
        host.accounts.entry(msg.recipient).or_default().balance = U256::ONE;
    });

    t.clone()
        .revision(Revision::Homestead)
        .gas_used(3)
        .status(StatusCode::Success)
        .inspect_host(|host, msg| {
            assert_eq!(
                host.recorded.account_accesses,
                [
                    // Selfdestruct.
                    msg.recipient
                ]
            );
        })
        .check();

    t.clone()
        .revision(Revision::Tangerine)
        .gas_used(30003)
        .status(StatusCode::Success)
        .inspect_host(move |host, msg| {
            assert_eq!(
                host.recorded.account_accesses,
                [
                    // Exists?
                    beneficiary,
                    // Selfdestruct.
                    msg.recipient
                ]
            );
        })
        .check();

    t.clone()
        .revision(Revision::Tangerine)
        .gas(30002)
        .status(StatusCode::OutOfGas)
        .inspect_host(move |host, _| {
            assert_eq!(
                host.recorded.account_accesses,
                [
                    // Exists?
                    beneficiary,
                ]
            );
        })
        .check();

    t.clone()
        .revision(Revision::Spurious)
        .gas_used(30003)
        .status(StatusCode::Success)
        .inspect_host(move |host, msg| {
            assert_eq!(
                host.recorded.account_accesses,
                [
                    // Balance
                    msg.recipient,
                    // Exists?
                    beneficiary,
                    // Selfdestruct.
                    msg.recipient
                ]
            );
        })
        .check();

    t.clone()
        .revision(Revision::Spurious)
        .gas(30002)
        .status(StatusCode::OutOfGas)
        .inspect_host(move |host, msg| {
            assert_eq!(
                host.recorded.account_accesses,
                [
                    // Balance
                    msg.recipient,
                    // Exists?
                    beneficiary,
                ]
            );
        })
        .check();

    t = t.apply_host_fn(move |host, msg| {
        host.accounts.entry(beneficiary).or_default(); // Beneficiary exists.
        host.accounts.get_mut(&msg.recipient).unwrap().balance = U256::ZERO;
    });

    t.clone()
        .revision(Revision::Homestead)
        .gas_used(3)
        .status(StatusCode::Success)
        .inspect_host(|host, msg| {
            assert_eq!(
                host.recorded.account_accesses,
                [
                    // Selfdestruct.
                    msg.recipient,
                ]
            );
        })
        .check();

    t.clone()
        .revision(Revision::Tangerine)
        .gas_used(5003)
        .status(StatusCode::Success)
        .inspect_host(move |host, msg| {
            assert_eq!(
                host.recorded.account_accesses,
                [
                    // Exists?
                    beneficiary,
                    // Selfdestruct.
                    msg.recipient,
                ]
            );
        })
        .check();

    t.clone()
        .revision(Revision::Tangerine)
        .gas(5002)
        .status(StatusCode::OutOfGas)
        .inspect_host(move |host, _| {
            assert_eq!(host.recorded.account_accesses, []);
        })
        .check();

    t.clone()
        .revision(Revision::Spurious)
        .gas(5003)
        .status(StatusCode::Success)
        .inspect_host(move |host, msg| {
            assert_eq!(
                host.recorded.account_accesses,
                [
                    // Balance.
                    msg.recipient,
                    // Selfdestruct.
                    msg.recipient,
                ]
            );
        })
        .check();

    t.clone()
        .revision(Revision::Spurious)
        .gas(5002)
        .status(StatusCode::OutOfGas)
        .inspect_host(|host, _| {
            assert_eq!(host.recorded.account_accesses, []);
        })
        .check();

    t = t.apply_host_fn(|host, msg| {
        host.accounts.entry(msg.recipient).or_default().balance = U256::ONE;
    });

    t.clone()
        .revision(Revision::Homestead)
        .gas_used(3)
        .status(StatusCode::Success)
        .inspect_host(|host, msg| {
            assert_eq!(
                host.recorded.account_accesses,
                [
                    // Selfdestruct
                    msg.recipient
                ]
            );
        })
        .check();

    t.clone()
        .revision(Revision::Tangerine)
        .gas_used(5003)
        .status(StatusCode::Success)
        .inspect_host(move |host, msg| {
            assert_eq!(
                host.recorded.account_accesses,
                [
                    // Exists?
                    beneficiary,
                    // Selfdestruct
                    msg.recipient
                ]
            );
        })
        .check();

    t.clone()
        .revision(Revision::Tangerine)
        .gas(5002)
        .status(StatusCode::OutOfGas)
        .inspect_host(|host, _| {
            assert_eq!(host.recorded.account_accesses, []);
        })
        .check();

    t.clone()
        .revision(Revision::Spurious)
        .gas_used(5003)
        .status(StatusCode::Success)
        .inspect_host(move |host, msg| {
            assert_eq!(
                host.recorded.account_accesses,
                [
                    // Balance
                    msg.recipient,
                    // Exists?
                    beneficiary,
                    // Selfdestruct
                    msg.recipient
                ]
            );
        })
        .check();

    t.revision(Revision::Spurious)
        .gas(5002)
        .status(StatusCode::OutOfGas)
        .inspect_host(|host, _| {
            assert_eq!(host.recorded.account_accesses, []);
        })
        .check();
}

#[test]
fn blockhash() {
    let t = EvmTester::new()
        .code(hex!("60004060005260206000f3"))
        .status(StatusCode::Success)
        .gas_used(38)
        .apply_host_fn(|host, _| {
            let mut v = host.block_hash.to_be_bytes();
            v[13] = 0x13;
            host.block_hash = U256::from_be_bytes(v);
        });

    t.clone()
        .apply_host_fn(|host, _| {
            host.tx_context.block_number = 0;
        })
        .inspect_output(|output| {
            assert_eq!(output.len(), 32);
            assert_eq!(output[13], 0);
        })
        .inspect_host(|host, _| {
            assert_eq!(host.recorded.blockhashes, [] as [u64; 0]);
        })
        .check();

    t.clone()
        .apply_host_fn(|host, _| {
            host.tx_context.block_number = 257;
        })
        .inspect_output(|output| {
            assert_eq!(output.len(), 32);
            assert_eq!(output[13], 0);
        })
        .inspect_host(|host, _| {
            assert_eq!(host.recorded.blockhashes, [] as [u64; 0]);
        })
        .check();

    t.apply_host_fn(|host, _| {
        host.tx_context.block_number = 256;
    })
    .inspect_output(|output| {
        assert_eq!(output.len(), 32);
        assert_eq!(output[13], 0x13);
    })
    .inspect_host(|host, _| {
        assert_eq!(host.recorded.blockhashes, [0]);
    })
    .check();
}

#[test]
fn extcode() {
    let addr = hex!("fffffffffffffffffffffffffffffffffffffffe").into();

    EvmTester::new()
        .apply_host_fn(move |host, _| {
            host.accounts.entry(addr).or_default().code = (&hex!("0a0b0c0d") as &[u8]).into();
        })
        .code(
            Bytecode::new()
                .append(hex!("6002600003803b60019003")) // S = EXTCODESIZE(-2) - 1
                .append(hex!("90600080913c")) // EXTCODECOPY(-2, 0, 0, S)
                .append(hex!("60046000f3")), // RETURN(0, 4)
        )
        .gas_used(1445)
        .status(StatusCode::Success)
        .inspect(move |host, _, output| {
            assert_eq!(output.len(), 4);
            assert_eq!(output[..3], host.accounts[&addr].code[..3]);
            assert_eq!(output[3], 0);
            assert_eq!(host.recorded.account_accesses.len(), 2);
            assert_eq!(host.recorded.account_accesses[0].0[19], 0xfe);
            assert_eq!(host.recorded.account_accesses[1].0[19], 0xfe);
        })
        .check()
}

#[test]
fn extcodesize() {
    EvmTester::new()
        .apply_host_fn(|host, _| {
            host.accounts
                .entry(hex!("0000000000000000000000000000000000000002").into())
                .or_default()
                .code = (&[0_u8] as &[u8]).into();
        })
        .code(
            Bytecode::new()
                .pushv(2)
                .opcode(OpCode::EXTCODESIZE)
                .ret_top(),
        )
        .output_value(1)
        .check()
}

#[test]
fn extcodecopy_big_index() {
    EvmTester::new()
        .code(
            Bytecode::new()
                .pushv(1)
                .opcode(OpCode::DUP1)
                .pushv(u64::from(u32::MAX) + 1)
                .pushv(0)
                .opcode(OpCode::DUP1)
                .opcode(OpCode::EXTCODECOPY)
                .pushv(0)
                .opcode(OpCode::RETURN),
        )
        .output_data(hex!("00"))
        .check()
}

#[test]
fn extcodehash() {
    let t = EvmTester::new()
        .apply_host_fn(|host, _| {
            host.accounts.entry(Address::zero()).or_default().code_hash = U256::from_be_bytes(
                hex!("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"),
            );
        })
        .code(hex!("60003f60005260206000f3"));

    t.clone()
        .revision(Revision::Byzantium)
        .status(StatusCode::UndefinedInstruction)
        .check();

    t.revision(Revision::Constantinople)
        .status(StatusCode::Success)
        .gas_used(418)
        .inspect(|host, _, output| {
            assert_eq!(
                output,
                host.accounts[&Address::zero()].code_hash.to_be_bytes()
            );
        })
        .check()
}

#[test]
fn codecopy_empty() {
    EvmTester::new()
        .code(
            Bytecode::new()
                .pushv(0)
                .opcode(OpCode::DUP1)
                .opcode(OpCode::DUP1)
                .opcode(OpCode::CODECOPY)
                .opcode(OpCode::MSIZE)
                .ret_top(),
        )
        .status(StatusCode::Success)
        .output_value(0)
        .check()
}

#[test]
fn extcodecopy_empty() {
    EvmTester::new()
        .code(
            Bytecode::new()
                .pushv(0_u128)
                .opcode(OpCode::DUP1)
                .opcode(OpCode::DUP1)
                .opcode(OpCode::DUP1)
                .opcode(OpCode::EXTCODECOPY)
                .opcode(OpCode::MSIZE)
                .ret_top(),
        )
        .status(StatusCode::Success)
        .output_value(U256::ZERO)
        .check()
}

#[test]
fn codecopy_memory_cost() {
    EvmTester::new()
        .code(
            Bytecode::new()
                .pushv(1_u128)
                .pushv(0_u128)
                .pushv(0_u128)
                .opcode(OpCode::CODECOPY),
        )
        .status(StatusCode::Success)
        .gas_used(18)
        .check()
}

#[test]
fn extcodecopy_memory_cost() {
    EvmTester::new()
        .code(
            Bytecode::new()
                .pushv(1_u128)
                .pushv(0_u128)
                .opcode(OpCode::DUP1)
                .opcode(OpCode::DUP1)
                .opcode(OpCode::EXTCODECOPY),
        )
        .gas_used(718)
        .check()
}

#[test]
fn extcodecopy_nonzero_index() {
    let index = 15;
    let code = Bytecode::new()
        .pushv(2)
        .pushv(index)
        .pushv(0_u128)
        .pushv(0xa)
        .opcode(OpCode::EXTCODECOPY)
        .ret(0, 2)
        .build();
    assert_eq!(code.len() + 1, index);

    EvmTester::new()
        .apply_host_fn(move |host, _| {
            let mut code = std::iter::repeat(0).take(16).collect::<Vec<u8>>();
            code[index] = 0xc0;
            host.accounts
                .entry(hex!("000000000000000000000000000000000000000a").into())
                .or_default()
                .code = code.into();
        })
        .code(code)
        .status(StatusCode::Success)
        .output_data(hex!("c000"))
        .inspect_host(|host, _| {
            assert_eq!(
                host.recorded.account_accesses,
                [hex!("000000000000000000000000000000000000000a").into()]
            );
        })
        .check()
}

#[test]
fn extcodecopy_fill_tail() {
    EvmTester::new()
        .apply_host_fn(|host, _| {
            let mut addr = Address::zero();
            addr.0[19] = 0xa;
            host.accounts.entry(addr).or_default().code = (&hex!("ff") as &[u8]).into();
        })
        .code(
            Bytecode::new()
                .pushv(2)
                .pushv(0_u128)
                .pushv(0_u128)
                .pushv(0xa)
                .opcode(OpCode::EXTCODECOPY)
                .ret(0, 2),
        )
        .status(StatusCode::Success)
        .output_data(hex!("ff00"))
        .inspect_host(|host, _| {
            assert_eq!(
                host.recorded.account_accesses,
                [hex!("000000000000000000000000000000000000000a").into()]
            );
        })
        .check()
}

#[test]
fn extcodecopy_buffer_overflow() {
    let code = Bytecode::new()
        .opcode(OpCode::NUMBER)
        .opcode(OpCode::TIMESTAMP)
        .opcode(OpCode::CALLDATASIZE)
        .opcode(OpCode::ADDRESS)
        .opcode(OpCode::EXTCODECOPY)
        .opcode(OpCode::NUMBER)
        .opcode(OpCode::CALLDATASIZE)
        .opcode(OpCode::RETURN)
        .build();

    let t = EvmTester::new().apply_host_fn({
        let code = code.clone();
        move |host, msg| {
            host.accounts.entry(msg.recipient).or_default().code = code.clone().into();
        }
    });

    let s = code.len();
    let values = [0, 1, s - 1, s, s + 1, 5000];
    for offset in values {
        for size in values {
            t.clone()
                .apply_host_fn(move |host, _| {
                    host.tx_context.block_timestamp = offset as u64;
                    host.tx_context.block_number = size as u64;
                })
                .code(code.clone())
                .status(StatusCode::Success)
                .inspect_output(move |output| {
                    assert_eq!(output.len(), size);
                })
                .check();
        }
    }
}
