use super::{address::*, precompiled};
use crate::{
    chain::protocol_param::{fee, param},
    common::{self, ADDRESS_LENGTH},
    models::*,
    IntraBlockState, State,
};
use anyhow::Context;
use async_recursion::async_recursion;
use bytes::Bytes;
use ethereum_types::{Address, H256, U256};
use evmodin::{
    continuation::{interrupt::*, interrupt_data::*, resume_data::*, Interrupt},
    host::*,
    CallKind, CreateMessage, Message, Output, Revision, StatusCode,
};
use std::cmp::min;

pub struct CallResult {
    /// EVM exited with this status code.
    pub status_code: StatusCode,
    /// How much gas was left after execution
    pub gas_left: i64,
    /// Output data returned.
    pub output_data: Bytes<'static>,
}

struct Evm<'storage, 'r, 'state, 'h, 'c, 't, B>
where
    B: State<'storage>,
{
    state: &'state mut IntraBlockState<'storage, 'r, B>,
    header: &'h BlockHeader,
    revision: Revision,
    chain_config: &'c ChainConfig,
    txn: &'t TransactionWithSender,
    address_stack: Vec<Address>,
}

pub async fn execute<'storage, 'r, B: State<'storage>>(
    state: &mut IntraBlockState<'storage, 'r, B>,
    header: &BlockHeader,
    chain_config: &ChainConfig,
    txn: &TransactionWithSender,
    gas: u64,
) -> anyhow::Result<CallResult> {
    let revision = chain_config.revision(header.number);
    let mut evm = Evm {
        header,
        state,
        revision,
        chain_config,
        txn,
        address_stack: Vec::new(),
    };

    let res = if let TransactionAction::Call(to) = txn.action {
        evm.call(Message {
            kind: CallKind::Call,
            is_static: false,
            depth: 0,
            sender: txn.sender,
            input_data: txn.input.clone().into(),
            value: txn.value,
            gas: gas as i64,
            destination: to,
        })
        .await?
    } else {
        evm.create(CreateMessage {
            depth: 0,
            gas: gas as i64,
            sender: txn.sender,
            initcode: txn.input.clone().into(),
            endowment: txn.value,
            salt: None,
        })
        .await?
    };

    Ok(CallResult {
        status_code: res.status_code,
        gas_left: res.gas_left,
        output_data: res.output_data.into(),
    })
}

impl<'storage, 'r, 'state, 'h, 'c, 't, B> Evm<'storage, 'r, 'state, 'h, 'c, 't, B>
where
    B: State<'storage>,
{
    #[async_recursion]
    async fn create(&mut self, message: CreateMessage) -> anyhow::Result<Output> {
        let mut res = Output {
            status_code: StatusCode::Success,
            gas_left: message.gas,
            output_data: Bytes::new().into(),
            create_address: None,
        };

        let value = message.endowment;
        if self.state.get_balance(message.sender).await? < value {
            res.status_code = StatusCode::InsufficientBalance;
            return Ok(res);
        }

        let nonce = self.state.get_nonce(message.sender).await?;
        self.state.set_nonce(message.sender, nonce + 1).await?;

        let contract_addr = {
            if let Some(salt) = message.salt {
                create2_address(
                    message.sender,
                    salt,
                    common::hash_data(message.initcode.as_ref()),
                )
            } else {
                create_address(message.sender, nonce)
            }
        };

        self.state.access_account(contract_addr);

        if self.state.get_nonce(contract_addr).await? != 0
            || self.state.get_code_hash(contract_addr).await? != common::EMPTY_HASH
        {
            // https://github.com/ethereum/EIPs/issues/684
            res.status_code = StatusCode::InvalidInstruction;
            res.gas_left = 0;
            return Ok(res);
        }

        let snapshot = self.state.take_snapshot();

        self.state.create_contract(contract_addr).await?;

        if self.revision >= Revision::Spurious {
            self.state.set_nonce(contract_addr, 1).await?;
        }

        self.state
            .subtract_from_balance(message.sender, value)
            .await?;
        self.state.add_to_balance(contract_addr, value).await?;

        let deploy_message = Message {
            kind: CallKind::Call,
            is_static: false,
            depth: message.depth,
            gas: message.gas,
            destination: contract_addr,
            sender: message.sender,
            input_data: Default::default(),
            value: message.endowment,
        };

        res = self
            .execute(deploy_message, message.initcode.as_ref().to_vec())
            .await?;

        if res.status_code == StatusCode::Success {
            let code_len = res.output_data.len();
            let code_deploy_gas = code_len as u64 * fee::G_CODE_DEPOSIT;

            if self.revision >= Revision::London && code_len > 0 && res.output_data[0] == 0xEF {
                // https://eips.ethereum.org/EIPS/eip-3541
                res.status_code = StatusCode::ContractValidationFailure;
            } else if self.revision >= Revision::Spurious && code_len > param::MAX_CODE_SIZE {
                // https://eips.ethereum.org/EIPS/eip-170
                res.status_code = StatusCode::OutOfGas;
            } else if res.gas_left >= 0 && res.gas_left as u64 >= code_deploy_gas {
                res.gas_left -= code_deploy_gas as i64;
                self.state
                    .set_code(contract_addr, res.output_data.clone().into())
                    .await?;
            } else if self.revision >= Revision::Homestead {
                res.status_code = StatusCode::OutOfGas;
            }
        }

        if res.status_code == StatusCode::Success {
            res.create_address = Some(contract_addr);
        } else {
            self.state.revert_to_snapshot(snapshot);
            if res.status_code != StatusCode::Revert {
                res.gas_left = 0;
            }
        }

        Ok(res)
    }

    #[async_recursion]
    async fn call(&mut self, message: Message) -> anyhow::Result<Output> {
        let mut res = Output {
            status_code: StatusCode::Success,
            gas_left: message.gas,
            output_data: Bytes::new().into(),
            create_address: None,
        };

        let value = message.value;
        if message.kind != CallKind::DelegateCall
            && self.state.get_balance(message.sender).await? < value
        {
            res.status_code = StatusCode::InsufficientBalance;
            return Ok(res);
        }

        // See Section 8 "Message Call" of the Yellow Paper for the difference between code & recipient.
        // destination in evmc_message can mean either code or recipient, depending on the context.
        let code_address = message.destination;
        let recipient_address = self.recipient_of_call_message(&message);

        let precompiled = self.is_precompiled(code_address);

        // https://eips.ethereum.org/EIPS/eip-161
        if value.is_zero()
            && self.revision >= Revision::Spurious
            && !precompiled
            && !self.state.exists(code_address).await?
        {
            return Ok(res);
        }

        let snapshot = self.state.take_snapshot();

        if message.kind == CallKind::Call {
            if message.is_static {
                // Match geth logic
                // https://github.com/ethereum/go-ethereum/blob/v1.9.25/core/vm/evm.go#L391
                self.state.touch(recipient_address);
            } else {
                self.state
                    .subtract_from_balance(message.sender, value)
                    .await?;
                self.state.add_to_balance(recipient_address, value).await?;
            }
        }

        if precompiled {
            let num = code_address.0[ADDRESS_LENGTH - 1] as usize;
            let contract = &precompiled::CONTRACTS[num - 1];
            let input = message.input_data;
            let gas = (contract.gas)(input.as_ref(), self.revision) as i64;
            if gas < 0 || gas > message.gas {
                res.status_code = StatusCode::OutOfGas;
            } else if let Some(output) = (contract.run)(input.into()) {
                res.status_code = StatusCode::Success;
                res.gas_left = message.gas - gas;
                res.output_data = output.into();
            } else {
                res.status_code = StatusCode::PrecompileFailure;
            }
        } else {
            let code = self.state.get_code(code_address).await?.unwrap_or_default();
            if code.is_empty() {
                return Ok(res);
            }

            let mut msg = message;
            msg.destination = recipient_address;

            res = self.execute(msg, code.as_ref().to_vec()).await?;
        }

        if res.status_code != StatusCode::Success {
            self.state.revert_to_snapshot(snapshot);
            if res.status_code != StatusCode::Revert {
                res.gas_left = 0;
            }
        }

        Ok(res)
    }

    fn recipient_of_call_message(&self, message: &Message) -> Address {
        match message.kind {
            CallKind::CallCode => message.sender,
            CallKind::DelegateCall => {
                // An evmc_message contains only two addresses (sender and "destination").
                // However, in case of DELEGATECALL we need 3 addresses (sender, code, and recipient),
                // so we recover the missing recipient address from the address stack.
                *self.address_stack.last().unwrap()
            }
            CallKind::Call => message.destination,
            _ => unreachable!(),
        }
    }

    async fn execute(&mut self, msg: Message, code: Vec<u8>) -> anyhow::Result<Output> {
        // msg.destination here means recipient (what ADDRESS opcode would return)
        self.address_stack.push(msg.destination);

        let mut interrupt = evmodin::AnalyzedCode::analyze(code)
            .execute_resumable(false, msg, self.revision)
            .resume(());

        let output = loop {
            interrupt = match interrupt {
                InterruptVariant::InstructionStart(_) => unreachable!("tracing is disabled"),
                InterruptVariant::AccountExists(i) => {
                    let address = i.data().address;
                    let exists = if self.revision >= Revision::Spurious {
                        !self.state.is_dead(address).await?
                    } else {
                        self.state.exists(address).await?
                    };
                    i.resume(AccountExistsStatus { exists })
                }
                InterruptVariant::GetBalance(i) => {
                    let balance = self.state.get_balance(i.data().address).await?;
                    i.resume(Balance { balance })
                }
                InterruptVariant::GetCodeSize(i) => {
                    let code_size = self
                        .state
                        .get_code(i.data().address)
                        .await?
                        .map(|c| c.len())
                        .unwrap_or(0)
                        .into();
                    i.resume(CodeSize { code_size })
                }
                InterruptVariant::GetStorage(i) => {
                    let value = self
                        .state
                        .get_current_storage(i.data().address, i.data().key)
                        .await?;
                    i.resume(StorageValue { value })
                }
                InterruptVariant::SetStorage(i) => {
                    let &SetStorage {
                        address,
                        key,
                        value: new_val,
                    } = i.data();

                    let current_val = self.state.get_current_storage(address, key).await?;

                    let status = if current_val == new_val {
                        StorageStatus::Unchanged
                    } else {
                        self.state.set_storage(address, key, new_val).await?;

                        let eip1283 = self.revision >= Revision::Istanbul
                            || self.revision == Revision::Constantinople;

                        if !eip1283 {
                            if current_val.is_zero() {
                                StorageStatus::Added
                            } else if new_val.is_zero() {
                                self.state.add_refund(fee::R_SCLEAR);
                                StorageStatus::Deleted
                            } else {
                                StorageStatus::Modified
                            }
                        } else {
                            let sload_cost = {
                                if self.revision >= Revision::Berlin {
                                    fee::WARM_STORAGE_READ_COST
                                } else if self.revision >= Revision::Istanbul {
                                    fee::G_SLOAD_ISTANBUL
                                } else {
                                    fee::G_SLOAD_TANGERINE_WHISTLE
                                }
                            };

                            let mut sstore_reset_gas = fee::G_SRESET;
                            if self.revision >= Revision::Berlin {
                                sstore_reset_gas -= fee::COLD_SLOAD_COST;
                            }

                            // https://eips.ethereum.org/EIPS/eip-1283
                            let original_val =
                                self.state.get_original_storage(address, key).await?;

                            // https://eips.ethereum.org/EIPS/eip-3529
                            let sstore_clears_refund = if self.revision >= Revision::London {
                                sstore_reset_gas + fee::ACCESS_LIST_STORAGE_KEY_COST
                            } else {
                                fee::R_SCLEAR
                            };

                            if original_val == current_val {
                                if original_val.is_zero() {
                                    StorageStatus::Added
                                } else {
                                    if new_val.is_zero() {
                                        self.state.add_refund(sstore_clears_refund);
                                    }
                                    StorageStatus::Modified
                                }
                            } else {
                                if !original_val.is_zero() {
                                    if current_val.is_zero() {
                                        self.state.subtract_refund(sstore_clears_refund);
                                    }
                                    if new_val.is_zero() {
                                        self.state.add_refund(sstore_clears_refund);
                                    }
                                }
                                if original_val == new_val {
                                    let refund = {
                                        if original_val.is_zero() {
                                            fee::G_SSET - sload_cost
                                        } else {
                                            sstore_reset_gas - sload_cost
                                        }
                                    };

                                    self.state.add_refund(refund);
                                }
                                StorageStatus::ModifiedAgain
                            }
                        }
                    };

                    i.resume(StorageStatusInfo { status })
                }
                InterruptVariant::GetCodeHash(i) => {
                    let address = i.data().address;
                    let hash = {
                        if self.state.is_dead(address).await? {
                            H256::zero()
                        } else {
                            self.state.get_code_hash(address).await?
                        }
                    };
                    i.resume(CodeHash { hash })
                }
                InterruptVariant::CopyCode(i) => {
                    let &CopyCode {
                        address,
                        offset,
                        max_size,
                    } = i.data();

                    let mut buffer = vec![0; max_size];

                    let code = self
                        .state
                        .get_code(address)
                        .await?
                        .unwrap_or_else(bytes::Bytes::new);

                    let mut copied = 0;
                    if offset < code.len() {
                        copied = min(max_size, code.len() - offset);
                        buffer[..copied].copy_from_slice(&code[offset..offset + copied]);
                    }

                    buffer.truncate(copied);
                    let code = buffer.into();

                    i.resume(Code { code })
                }
                InterruptVariant::Selfdestruct(i) => {
                    self.state.record_selfdestruct(i.data().address);
                    let balance = self.state.get_balance(i.data().address).await?;
                    self.state
                        .add_to_balance(i.data().beneficiary, balance)
                        .await?;
                    self.state.set_balance(i.data().address, 0).await?;

                    i.resume(())
                }
                InterruptVariant::Call(i) => {
                    let output = match i.data() {
                        Call::Create(message) => {
                            let res = self.create(message.clone()).await?;

                            // https://eips.ethereum.org/EIPS/eip-211
                            if res.status_code == StatusCode::Revert {
                                // geth returns CREATE output only in case of REVERT
                                res
                            } else {
                                Output {
                                    output_data: Default::default(),
                                    ..res
                                }
                            }
                        }
                        Call::Call(message) => self.call(message.clone()).await?,
                    };

                    i.resume(CallOutput { output })
                }
                InterruptVariant::GetTxContext(i) => {
                    let base_fee_per_gas = self.header.base_fee_per_gas.unwrap_or_else(U256::zero);
                    let tx_gas_price = self.txn.effective_gas_price(base_fee_per_gas);
                    let tx_origin = self.txn.sender;
                    let block_coinbase = self.header.beneficiary;
                    let block_number = self.header.number;
                    let block_timestamp = self.header.timestamp;
                    let block_gas_limit = self.header.gas_limit;
                    let block_difficulty = self.header.difficulty;
                    let chain_id = self.chain_config.chain_id.into();
                    let block_base_fee = base_fee_per_gas;

                    let context = TxContext {
                        tx_gas_price,
                        tx_origin,
                        block_coinbase,
                        block_number,
                        block_timestamp,
                        block_gas_limit,
                        block_difficulty,
                        chain_id,
                        block_base_fee,
                    };

                    i.resume(TxContextData { context })
                }
                InterruptVariant::GetBlockHash(i) => {
                    let n = i.data().block_number;

                    let base_number = self.header.number;
                    let distance = base_number - n;
                    assert!(distance <= 256);

                    let mut hash = self.header.parent_hash;

                    for i in 1..distance {
                        hash = self
                            .state
                            .db()
                            .read_header(base_number - i, hash)
                            .await?
                            .context("no header")?
                            .parent_hash;
                    }

                    i.resume(BlockHash { hash })
                }
                InterruptVariant::EmitLog(i) => {
                    self.state.add_log(Log {
                        address: i.data().address,
                        topics: i.data().topics.as_slice().into(),
                        data: i.data().data.clone().into(),
                    });

                    i.resume(())
                }
                InterruptVariant::AccessAccount(i) => {
                    let address = i.data().address;

                    let status = if self.is_precompiled(address) {
                        AccessStatus::Warm
                    } else {
                        self.state.access_account(address)
                    };
                    i.resume(AccessAccountStatus { status })
                }
                InterruptVariant::AccessStorage(i) => {
                    let status = self.state.access_storage(i.data().address, i.data().key);
                    i.resume(AccessStorageStatus { status })
                }
                InterruptVariant::Complete(i) => {
                    let output = match i {
                        Ok(output) => output.into(),
                        Err(status_code) => Output {
                            status_code,
                            gas_left: 0,
                            output_data: static_bytes::Bytes::new(),
                            create_address: None,
                        },
                    };

                    break output;
                }
            };
        };

        self.address_stack.pop();

        Ok(output)
    }

    fn number_of_precompiles(&self) -> u8 {
        match self.revision {
            Revision::Frontier | Revision::Homestead | Revision::Tangerine | Revision::Spurious => {
                precompiled::NUM_OF_FRONTIER_CONTRACTS as u8
            }
            Revision::Byzantium | Revision::Constantinople | Revision::Petersburg => {
                precompiled::NUM_OF_BYZANTIUM_CONTRACTS as u8
            }
            Revision::Istanbul | Revision::Berlin | Revision::London | Revision::Shanghai => {
                precompiled::NUM_OF_ISTANBUL_CONTRACTS as u8
            }
        }
    }

    fn is_precompiled(&self, contract: Address) -> bool {
        if contract.is_zero() {
            false
        } else {
            let mut max_precompiled = Address::zero();
            max_precompiled.0[ADDRESS_LENGTH - 1] = self.number_of_precompiles() as u8;
            contract <= max_precompiled
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        chain::config::MAINNET_CONFIG, common::ETHER, util::test_util::run_test, InMemoryState,
    };
    use hex_literal::hex;

    async fn execute<'storage, 'r, B: State<'storage>>(
        state: &mut IntraBlockState<'storage, 'r, B>,
        header: &BlockHeader,
        txn: &TransactionWithSender,
        gas: u64,
    ) -> CallResult {
        super::execute(state, header, &MAINNET_CONFIG, txn, gas)
            .await
            .unwrap()
    }

    #[test]
    fn value_transfer() {
        run_test(async {
            let header = BlockHeader {
                number: 10_336_006,
                ..BlockHeader::empty()
            };

            let sender = hex!("0a6bb546b9208cfab9e8fa2b9b2c042b18df7030").into();
            let to = hex!("8b299e2b7d7f43c0ce3068263545309ff4ffb521").into();
            let value = 10_200_000_000_000_000_u64.into();

            let mut db = InMemoryState::default();
            let mut state = IntraBlockState::new(&mut db);

            assert_eq!(state.get_balance(sender).await.unwrap(), U256::zero());
            assert_eq!(state.get_balance(to).await.unwrap(), U256::zero());

            let txn = TransactionWithSender {
                tx_type: TxType::Legacy,
                sender,
                action: TransactionAction::Call(to),
                value,
                ..TransactionWithSender::empty()
            };

            let gas = 0;

            let res = execute(&mut state, &header, &txn, gas).await;
            assert_eq!(res.status_code, StatusCode::InsufficientBalance);
            assert_eq!(res.output_data, []);

            state.add_to_balance(sender, ETHER).await.unwrap();

            let res = execute(&mut state, &header, &txn, gas).await;
            assert_eq!(res.status_code, StatusCode::Success);
            assert_eq!(res.output_data, []);

            assert_eq!(
                state.get_balance(sender).await.unwrap(),
                U256::from(ETHER) - value
            );
            assert_eq!(state.get_balance(to).await.unwrap(), value);
        })
    }

    #[test]
    fn smart_contract_with_storage() {
        run_test(async {
            let header = BlockHeader {
                number: 10_336_006,
                ..BlockHeader::empty()
            };
            let caller = hex!("0a6bb546b9208cfab9e8fa2b9b2c042b18df7030").into();

            // This contract initially sets its 0th storage to 0x2a
            // and its 1st storage to 0x01c9.
            // When called, it updates the 0th storage to the input provided.
            let code = hex!("602a6000556101c960015560068060166000396000f3600035600055");

            // https://github.com/CoinCulture/evm-tools
            // 0      PUSH1  => 2a
            // 2      PUSH1  => 00
            // 4      SSTORE         // storage[0] = 0x2a
            // 5      PUSH2  => 01c9
            // 8      PUSH1  => 01
            // 10     SSTORE         // storage[1] = 0x01c9
            // 11     PUSH1  => 06   // deploy begin
            // 13     DUP1
            // 14     PUSH1  => 16
            // 16     PUSH1  => 00
            // 18     CODECOPY
            // 19     PUSH1  => 00
            // 21     RETURN         // deploy end
            // 22     PUSH1  => 00   // contract code
            // 24     CALLDATALOAD
            // 25     PUSH1  => 00
            // 27     SSTORE         // storage[0] = input[0]

            let mut db = InMemoryState::default();
            let mut state = IntraBlockState::new(&mut db);

            let mut txn = TransactionWithSender {
                sender: caller,
                input: code.to_vec().into(),
                ..TransactionWithSender::empty()
            };

            let gas = 0;
            let res = execute(&mut state, &header, &txn, gas).await;
            assert_eq!(res.status_code, StatusCode::OutOfGas);
            assert_eq!(res.output_data, Bytes::new());

            let gas = 50_000;
            let res = execute(&mut state, &header, &txn, gas).await;
            assert_eq!(res.status_code, StatusCode::Success);
            assert_eq!(res.output_data, hex!("600035600055"));

            let contract_address = create_address(caller, 1);
            let key0 = H256::zero();
            assert_eq!(
                state
                    .get_current_storage(contract_address, key0)
                    .await
                    .unwrap(),
                H256::from_low_u64_be(0x2a)
            );

            let new_val = H256::from_low_u64_be(0xf5);
            txn.action = TransactionAction::Call(contract_address);
            txn.input = new_val.0.to_vec().into();

            let res = execute(&mut state, &header, &txn, gas).await;
            assert_eq!(res.status_code, StatusCode::Success);
            assert_eq!(res.output_data, []);
            assert_eq!(
                state
                    .get_current_storage(contract_address, key0)
                    .await
                    .unwrap(),
                new_val
            );
        })
    }

    #[test]
    fn maximum_call_depth() {
        run_test(async {
            let header = BlockHeader {
                number: 1_431_916,
                ..BlockHeader::empty()
            };
            let caller = hex!("8e4d1ea201b908ab5e1f5a1c3f9f1b4f6c1e9cf1").into();
            let contract = hex!("3589d05a1ec4af9f65b0e5554e645707775ee43c").into();

            // The contract just calls itself recursively a given number of times.
            let code =
                hex!("60003580600857005b6001900360005260008060208180305a6103009003f1602357fe5b");

            //     https://github.com/CoinCulture/evm-tools
            //     0      PUSH1  => 00
            //     2      CALLDATALOAD
            //     3      DUP1
            //     4      PUSH1  => 08
            //     6      JUMPI
            //     7      STOP
            //     8      JUMPDEST
            //     9      PUSH1  => 01
            //     11     SWAP1
            //     12     SUB
            //     13     PUSH1  => 00
            //     15     MSTORE
            //     16     PUSH1  => 00
            //     18     DUP1
            //     19     PUSH1  => 20
            //     21     DUP2
            //     22     DUP1
            //     23     ADDRESS
            //     24     GAS
            //     25     PUSH2  => 0300
            //     28     SWAP1
            //     29     SUB
            //     30     CALL
            //     31     PUSH1  => 23
            //     33     JUMPI
            //     34     INVALID
            //     35     JUMPDEST

            let mut db = InMemoryState::default();
            let mut state = IntraBlockState::new(&mut db);

            state
                .set_code(contract, code.to_vec().into())
                .await
                .unwrap();

            let mut txn = TransactionWithSender {
                sender: caller,
                action: TransactionAction::Call(contract),
                ..TransactionWithSender::empty()
            };

            let gas = 1_000_000;
            let res = execute(&mut state, &header, &txn, gas).await;
            assert_eq!(res.status_code, StatusCode::Success);
            assert_eq!(res.output_data, []);

            let num_of_recursions = 0x0400;
            txn.input = H256::from_low_u64_be(num_of_recursions).0.to_vec().into();
            let res = execute(&mut state, &header, &txn, gas).await;
            assert_eq!(res.status_code, StatusCode::Success);
            assert_eq!(res.output_data, []);

            let num_of_recursions = 0x0401;
            txn.input = H256::from_low_u64_be(num_of_recursions).0.to_vec().into();
            let res = execute(&mut state, &header, &txn, gas).await;
            assert_eq!(res.status_code, StatusCode::InvalidInstruction);
            assert_eq!(res.output_data, []);
        })
    }

    #[test]
    fn delegatecall() {
        run_test(async {
            let header = BlockHeader {
                number: 1_639_560,
                ..BlockHeader::empty()
            };
            let caller_address = hex!("8e4d1ea201b908ab5e1f5a1c3f9f1b4f6c1e9cf1").into();
            let callee_address = hex!("3589d05a1ec4af9f65b0e5554e645707775ee43c").into();

            // The callee writes the ADDRESS to storage.
            let callee_code = hex!("30600055");
            // https://github.com/CoinCulture/evm-tools
            // 0      ADDRESS
            // 1      PUSH1  => 00
            // 3      SSTORE

            // The caller delegate-calls the input contract.
            let caller_code = hex!("6000808080803561eeeef4");
            // https://github.com/CoinCulture/evm-tools
            // 0      PUSH1  => 00
            // 2      DUP1
            // 3      DUP1
            // 4      DUP1
            // 5      DUP1
            // 6      CALLDATALOAD
            // 7      PUSH2  => eeee
            // 10     DELEGATECALL

            let mut db = InMemoryState::default();
            let mut state = IntraBlockState::new(&mut db);

            state
                .set_code(caller_address, caller_code.to_vec().into())
                .await
                .unwrap();
            state
                .set_code(callee_address, callee_code.to_vec().into())
                .await
                .unwrap();

            let txn = TransactionWithSender {
                sender: caller_address,
                action: TransactionAction::Call(caller_address),
                input: H256::from(callee_address).0.to_vec().into(),
                ..TransactionWithSender::empty()
            };

            let gas = 1_000_000;
            let res = execute(&mut state, &header, &txn, gas).await;
            assert_eq!(res.status_code, StatusCode::Success);
            assert_eq!(res.output_data, []);

            let key0 = H256::zero();
            assert_eq!(
                state
                    .get_current_storage(caller_address, key0)
                    .await
                    .unwrap(),
                caller_address.into()
            );
        })
    }

    // https://eips.ethereum.org/EIPS/eip-211#specification
    #[test]
    fn create_should_only_return_on_failure() {
        run_test(async {
            let header = BlockHeader {
                number: 4_575_910,
                ..BlockHeader::empty()
            };
            let caller = hex!("f466859ead1932d743d622cb74fc058882e8648a").into();

            let code = hex!("602180601360003960006000f0503d6000550062112233600052602060006020600060006004619000f1503d60005560206000f3");
            // https://github.com/CoinCulture/evm-tools
            // 0      PUSH1  => 21
            // 2      DUP1
            // 3      PUSH1  => 13
            // 5      PUSH1  => 00
            // 7      CODECOPY
            // 8      PUSH1  => 00
            // 10     PUSH1  => 00
            // 12     CREATE
            // 13     POP
            // 14     RETURNDATASIZE
            // 15     PUSH1  => 00
            // 17     SSTORE
            // 18     STOP
            // 19     PUSH3  => 112233
            // 23     PUSH1  => 00
            // 25     MSTORE
            // 26     PUSH1  => 20
            // 28     PUSH1  => 00
            // 30     PUSH1  => 20
            // 32     PUSH1  => 00
            // 34     PUSH1  => 00
            // 36     PUSH1  => 04
            // 38     PUSH2  => 9000
            // 41     CALL
            // 42     POP
            // 43     RETURNDATASIZE
            // 44     PUSH1  => 00
            // 46     SSTORE
            // 47     PUSH1  => 20
            // 49     PUSH1  => 00
            // 51     RETURN

            let mut db = InMemoryState::default();
            let mut state = IntraBlockState::new(&mut db);

            let txn = TransactionWithSender {
                sender: caller,
                input: code.to_vec().into(),
                ..TransactionWithSender::empty()
            };

            let gas = 150_000;
            let res = execute(&mut state, &header, &txn, gas).await;
            assert_eq!(res.status_code, StatusCode::Success);
            assert_eq!(res.output_data, []);

            let contract_address = create_address(caller, 0);
            let key0 = H256::zero();
            assert_eq!(
                state
                    .get_current_storage(contract_address, key0)
                    .await
                    .unwrap(),
                H256::zero()
            );
        })
    }

    // https://github.com/ethereum/EIPs/issues/684
    #[test]
    fn contract_overwrite() {
        run_test(async {
            let header = BlockHeader {
                number: 7_753_545,
                ..BlockHeader::empty()
            };

            let old_code = hex!("6000");
            let new_code = hex!("6001");

            let caller = hex!("92a1d964b8fc79c5694343cc943c27a94a3be131").into();

            let contract_address = create_address(caller, 0);

            let mut db = InMemoryState::default();
            let mut state = IntraBlockState::new(&mut db);
            state
                .set_code(contract_address, old_code.to_vec().into())
                .await
                .unwrap();

            let txn = TransactionWithSender {
                sender: caller,
                input: new_code.to_vec().into(),
                ..TransactionWithSender::empty()
            };

            let gas = 100_000;
            let res = execute(&mut state, &header, &txn, gas).await;

            assert_eq!(res.status_code, StatusCode::InvalidInstruction);
            assert_eq!(res.gas_left, 0);
            assert_eq!(res.output_data, []);
        })
    }

    #[test]
    fn eip3541() {
        run_test(async {
            let header = BlockHeader {
                number: 13_500_000,
                ..BlockHeader::empty()
            };

            let mut db = InMemoryState::default();
            let mut state = IntraBlockState::new(&mut db);

            let mut txn = TransactionWithSender {
                sender: hex!("1000000000000000000000000000000000000000").into(),
                ..TransactionWithSender::empty()
            };

            let gas = 50_000;

            // https://eips.ethereum.org/EIPS/eip-3541#test-cases
            txn.input = hex!("60ef60005360016000f3").to_vec().into();
            assert_eq!(
                execute(&mut state, &header, &txn, gas).await.status_code,
                StatusCode::ContractValidationFailure
            );

            txn.input = hex!("60ef60005360026000f3").to_vec().into();
            assert_eq!(
                execute(&mut state, &header, &txn, gas).await.status_code,
                StatusCode::ContractValidationFailure
            );

            txn.input = hex!("60ef60005360036000f3").to_vec().into();
            assert_eq!(
                execute(&mut state, &header, &txn, gas).await.status_code,
                StatusCode::ContractValidationFailure
            );

            txn.input = hex!("60ef60005360206000f3").to_vec().into();
            assert_eq!(
                execute(&mut state, &header, &txn, gas).await.status_code,
                StatusCode::ContractValidationFailure
            );

            txn.input = hex!("60fe60005360016000f3").to_vec().into();
            assert_eq!(
                execute(&mut state, &header, &txn, gas).await.status_code,
                StatusCode::Success
            );
        })
    }
}
