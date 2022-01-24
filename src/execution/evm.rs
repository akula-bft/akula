use super::{
    address::*,
    analysis_cache::AnalysisCache,
    precompiled,
    tracer::{CodeKind, MessageKind, Tracer},
};
use crate::{
    chain::protocol_param::{fee, param},
    h256_to_u256,
    models::*,
    u256_to_h256, IntraBlockState, State,
};
use anyhow::Context;
use async_recursion::async_recursion;
use bytes::Bytes;
use evmodin::{
    continuation::{interrupt::*, interrupt_data::*, resume_data::*, Interrupt},
    host::*,
    CallKind, CreateMessage, Message as EvmMessage, Output, Revision, StatusCode,
};
use sha3::{Digest, Keccak256};
use std::{cmp::min, convert::TryFrom};

pub struct CallResult {
    /// EVM exited with this status code.
    pub status_code: StatusCode,
    /// How much gas was left after execution
    pub gas_left: i64,
    /// Output data returned.
    pub output_data: Bytes,
}

struct Evm<'r, 'state, 'tracer, 'analysis, 'h, 'c, 't, B>
where
    B: State,
{
    state: &'state mut IntraBlockState<'r, B>,
    tracer: Option<&'tracer mut dyn Tracer>,
    analysis_cache: &'analysis mut AnalysisCache,
    header: &'h PartialHeader,
    block_spec: &'c BlockExecutionSpec,
    txn: &'t MessageWithSender,
    beneficiary: Address,
}

pub async fn execute<B: State>(
    state: &mut IntraBlockState<'_, B>,
    tracer: Option<&mut dyn Tracer>,
    analysis_cache: &mut AnalysisCache,
    header: &PartialHeader,
    block_spec: &BlockExecutionSpec,
    txn: &MessageWithSender,
    gas: u64,
) -> anyhow::Result<CallResult> {
    let mut evm = Evm {
        header,
        tracer,
        analysis_cache,
        state,
        block_spec,
        txn,
        beneficiary: header.beneficiary,
    };

    let res = if let TransactionAction::Call(to) = txn.action() {
        evm.call(EvmMessage {
            kind: CallKind::Call,
            is_static: false,
            depth: 0,
            sender: txn.sender,
            input_data: txn.input().clone(),
            value: txn.value(),
            gas: gas as i64,
            recipient: to,
            code_address: to,
        })
        .await?
    } else {
        evm.create(CreateMessage {
            depth: 0,
            gas: gas as i64,
            sender: txn.sender,
            initcode: txn.input().clone(),
            endowment: txn.value(),
            salt: None,
        })
        .await?
    };

    Ok(CallResult {
        status_code: res.status_code,
        gas_left: res.gas_left,
        output_data: res.output_data,
    })
}

impl<'r, 'state, 'tracer, 'analysis, 'h, 'c, 't, B>
    Evm<'r, 'state, 'tracer, 'analysis, 'h, 'c, 't, B>
where
    B: State,
{
    #[async_recursion]
    async fn create(&mut self, message: CreateMessage) -> anyhow::Result<Output> {
        let mut res = Output {
            status_code: StatusCode::Success,
            gas_left: message.gas,
            output_data: Bytes::new(),
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
                    H256::from_slice(&Keccak256::digest(&message.initcode[..])[..]),
                )
            } else {
                create_address(message.sender, nonce)
            }
        };

        self.state.access_account(contract_addr);

        if let Some(tracer) = self.tracer.as_mut() {
            tracer.capture_start(
                message.depth.try_into().unwrap(),
                message.sender,
                contract_addr,
                MessageKind::Create,
                message.initcode.clone(),
                message.gas.try_into().unwrap(),
                message.endowment,
            );
        };

        if self.state.get_nonce(contract_addr).await? != 0
            || self.state.get_code_hash(contract_addr).await? != EMPTY_HASH
        {
            // https://github.com/ethereum/EIPs/issues/684
            res.status_code = StatusCode::InvalidInstruction;
            res.gas_left = 0;
            return Ok(res);
        }

        let snapshot = self.state.take_snapshot();

        self.state.create_contract(contract_addr).await?;

        if self.block_spec.revision >= Revision::Spurious {
            self.state.set_nonce(contract_addr, 1).await?;
        }

        self.state
            .subtract_from_balance(message.sender, value)
            .await?;
        self.state.add_to_balance(contract_addr, value).await?;

        let deploy_message = EvmMessage {
            kind: CallKind::Call,
            is_static: false,
            depth: message.depth,
            gas: message.gas,
            recipient: contract_addr,
            code_address: Address::zero(),
            sender: message.sender,
            input_data: Default::default(),
            value: message.endowment,
        };

        res = self
            .execute(deploy_message, message.initcode.as_ref().to_vec(), None)
            .await?;

        if res.status_code == StatusCode::Success {
            let code_len = res.output_data.len();
            let code_deploy_gas = code_len as u64 * fee::G_CODE_DEPOSIT;

            if self.block_spec.revision >= Revision::London
                && code_len > 0
                && res.output_data[0] == 0xEF
            {
                // https://eips.ethereum.org/EIPS/eip-3541
                res.status_code = StatusCode::ContractValidationFailure;
            } else if self.block_spec.revision >= Revision::Spurious
                && code_len > param::MAX_CODE_SIZE
            {
                // https://eips.ethereum.org/EIPS/eip-170
                res.status_code = StatusCode::OutOfGas;
            } else if res.gas_left >= 0 && res.gas_left as u64 >= code_deploy_gas {
                res.gas_left -= code_deploy_gas as i64;
                self.state
                    .set_code(contract_addr, res.output_data.clone())
                    .await?;
            } else if self.block_spec.revision >= Revision::Homestead {
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
    async fn call(&mut self, message: EvmMessage) -> anyhow::Result<Output> {
        let mut res = Output {
            status_code: StatusCode::Success,
            gas_left: message.gas,
            output_data: Bytes::new(),
            create_address: None,
        };

        let value = message.value;
        if message.kind != CallKind::DelegateCall
            && self.state.get_balance(message.sender).await? < value
        {
            res.status_code = StatusCode::InsufficientBalance;
            return Ok(res);
        }

        let precompiled = self.is_precompiled(message.code_address);

        let code = if !precompiled {
            self.state.get_code(message.code_address).await?
        } else {
            None
        };

        if let Some(tracer) = &mut self.tracer {
            let call_kind = {
                match (message.kind, message.is_static) {
                    (CallKind::Call, true) => super::tracer::CallKind::StaticCall,
                    (CallKind::Call, false) => super::tracer::CallKind::Call,
                    (CallKind::CallCode, _) => super::tracer::CallKind::CallCode,
                    (CallKind::DelegateCall, _) => super::tracer::CallKind::DelegateCall,
                    _ => unreachable!(),
                }
            };
            tracer.capture_start(
                message.depth.try_into().unwrap(),
                message.sender,
                message.recipient,
                MessageKind::Call {
                    call_kind,
                    code_kind: if precompiled {
                        CodeKind::Precompile
                    } else {
                        CodeKind::Bytecode(None)
                    },
                },
                message.input_data.clone(),
                message.gas.try_into().unwrap(),
                value,
            )
        }

        // https://eips.ethereum.org/EIPS/eip-161
        if value == 0
            && self.block_spec.revision >= Revision::Spurious
            && !precompiled
            && !self.state.exists(message.code_address).await?
        {
            return Ok(res);
        }

        let snapshot = self.state.take_snapshot();

        if message.kind == CallKind::Call {
            if message.is_static {
                // Match geth logic
                // https://github.com/ethereum/go-ethereum/blob/v1.9.25/core/vm/evm.go#L391
                self.state.touch(message.recipient);
            } else {
                self.state
                    .subtract_from_balance(message.sender, value)
                    .await?;
                self.state.add_to_balance(message.recipient, value).await?;
            }
        }

        if precompiled {
            let num = message.code_address.0[ADDRESS_LENGTH - 1] as usize;
            let contract = &precompiled::CONTRACTS[num - 1];
            let input = message.input_data;
            if let Some(gas) = (contract.gas)(input.clone(), self.block_spec.revision)
                .and_then(|g| i64::try_from(g).ok())
            {
                if gas > message.gas {
                    res.status_code = StatusCode::OutOfGas;
                } else if let Some(output) = (contract.run)(input) {
                    res.status_code = StatusCode::Success;
                    res.gas_left = message.gas - gas;
                    res.output_data = output;
                } else {
                    res.status_code = StatusCode::PrecompileFailure;
                }
            } else {
                res.status_code = StatusCode::OutOfGas;
            }
        } else {
            let code = code.unwrap_or_default();
            if code.is_empty() {
                return Ok(res);
            }

            let code_hash = self.state.get_code_hash(message.code_address).await?;

            res = self
                .execute(message, code.as_ref().to_vec(), Some(code_hash))
                .await?;
        }

        if res.status_code != StatusCode::Success {
            self.state.revert_to_snapshot(snapshot);
            if res.status_code != StatusCode::Revert {
                res.gas_left = 0;
            }
        }

        Ok(res)
    }

    async fn execute(
        &mut self,
        msg: EvmMessage,
        code: Vec<u8>,
        code_hash: Option<H256>,
    ) -> anyhow::Result<Output> {
        let a;
        let analysis = if let Some(code_hash) = code_hash {
            if let Some(cache) = self.analysis_cache.get(code_hash) {
                cache
            } else {
                let analysis = evmodin::AnalyzedCode::analyze(code);
                self.analysis_cache.put(code_hash, analysis);
                self.analysis_cache.get(code_hash).unwrap()
            }
        } else {
            a = evmodin::AnalyzedCode::analyze(code);
            &a
        };

        let mut interrupt = analysis
            .execute_resumable(false, msg, self.block_spec.revision)
            .resume(());

        let output = loop {
            interrupt = match interrupt {
                InterruptVariant::InstructionStart(_) => unreachable!("tracing is disabled"),
                InterruptVariant::AccountExists(i) => {
                    let address = i.data().address;
                    let exists = if self.block_spec.revision >= Revision::Spurious {
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
                    let code_size = u64::try_from(
                        self.state
                            .get_code(i.data().address)
                            .await?
                            .map(|c| c.len())
                            .unwrap_or(0),
                    )?
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

                        let eip1283 = self.block_spec.revision >= Revision::Istanbul
                            || self.block_spec.revision == Revision::Constantinople;

                        if !eip1283 {
                            if current_val == 0 {
                                StorageStatus::Added
                            } else if new_val == 0 {
                                self.state.add_refund(fee::R_SCLEAR);
                                StorageStatus::Deleted
                            } else {
                                StorageStatus::Modified
                            }
                        } else {
                            let sload_cost = {
                                if self.block_spec.revision >= Revision::Berlin {
                                    fee::WARM_STORAGE_READ_COST
                                } else if self.block_spec.revision >= Revision::Istanbul {
                                    fee::G_SLOAD_ISTANBUL
                                } else {
                                    fee::G_SLOAD_TANGERINE_WHISTLE
                                }
                            };

                            let mut sstore_reset_gas = fee::G_SRESET;
                            if self.block_spec.revision >= Revision::Berlin {
                                sstore_reset_gas -= fee::COLD_SLOAD_COST;
                            }

                            // https://eips.ethereum.org/EIPS/eip-1283
                            let original_val =
                                self.state.get_original_storage(address, key).await?;

                            // https://eips.ethereum.org/EIPS/eip-3529
                            let sstore_clears_refund =
                                if self.block_spec.revision >= Revision::London {
                                    sstore_reset_gas + fee::ACCESS_LIST_STORAGE_KEY_COST
                                } else {
                                    fee::R_SCLEAR
                                };

                            if original_val == current_val {
                                if original_val == 0 {
                                    StorageStatus::Added
                                } else {
                                    if new_val == 0 {
                                        self.state.add_refund(sstore_clears_refund);
                                    }
                                    StorageStatus::Modified
                                }
                            } else {
                                if original_val != 0 {
                                    if current_val == 0 {
                                        self.state.subtract_refund(sstore_clears_refund);
                                    }
                                    if new_val == 0 {
                                        self.state.add_refund(sstore_clears_refund);
                                    }
                                }
                                if original_val == new_val {
                                    let refund = {
                                        if original_val == 0 {
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
                    let hash = h256_to_u256({
                        if self.state.is_dead(address).await? {
                            H256::zero()
                        } else {
                            self.state.get_code_hash(address).await?
                        }
                    });
                    i.resume(CodeHash { hash })
                }
                InterruptVariant::CopyCode(i) => {
                    let &CopyCode {
                        address,
                        offset,
                        max_size,
                    } = i.data();

                    let mut buffer = vec![0; max_size];

                    let code = self.state.get_code(address).await?.unwrap_or_default();

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

                    if let Some(tracer) = &mut self.tracer {
                        tracer.capture_self_destruct(i.data().address, i.data().beneficiary);
                    }

                    i.resume(())
                }
                InterruptVariant::Call(i) => {
                    let output = match i.data() {
                        Call::Create(message) => {
                            let mut res = self.create(message.clone()).await?;

                            // https://eips.ethereum.org/EIPS/eip-211
                            if res.status_code != StatusCode::Revert {
                                // geth returns CREATE output only in case of REVERT
                                res.output_data = Default::default();
                            }

                            res
                        }
                        Call::Call(message) => self.call(message.clone()).await?,
                    };

                    i.resume(CallOutput { output })
                }
                InterruptVariant::GetTxContext(i) => {
                    let base_fee_per_gas = self.header.base_fee_per_gas.unwrap_or(U256::ZERO);
                    let tx_gas_price = self.txn.effective_gas_price(base_fee_per_gas);
                    let tx_origin = self.txn.sender;
                    let block_coinbase = self.beneficiary;
                    let block_number = self.header.number.0;
                    let block_timestamp = self.header.timestamp;
                    let block_gas_limit = self.header.gas_limit;
                    let block_difficulty = self.header.difficulty;
                    let chain_id = self.block_spec.params.chain_id.0.into();
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
                    let distance = base_number.0 - n;
                    assert!(distance <= 256);

                    let mut hash = self.header.parent_hash;

                    for i in 1..distance {
                        hash = self
                            .state
                            .db()
                            .read_header(BlockNumber(base_number.0 - i), hash)
                            .await?
                            .context("no header")?
                            .parent_hash;
                    }

                    let hash = h256_to_u256(hash);
                    i.resume(BlockHash { hash })
                }
                InterruptVariant::EmitLog(i) => {
                    self.state.add_log(Log {
                        address: i.data().address,
                        topics: i.data().topics.iter().copied().map(u256_to_h256).collect(),
                        data: i.data().data.clone(),
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
                            output_data: bytes::Bytes::new(),
                            create_address: None,
                        },
                    };

                    break output;
                }
            };
        };

        Ok(output)
    }

    fn number_of_precompiles(&self) -> u8 {
        match self.block_spec.revision {
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
    use crate::{res::chainspec::MAINNET, util::test_util::run_test, InMemoryState};
    use bytes_literal::bytes;
    use hex_literal::hex;

    async fn execute<B: State>(
        state: &mut IntraBlockState<'_, B>,
        header: &PartialHeader,
        txn: &MessageWithSender,
        gas: u64,
    ) -> CallResult {
        super::execute(
            state,
            None,
            &mut AnalysisCache::default(),
            header,
            &MAINNET.collect_block_spec(header.number),
            txn,
            gas,
        )
        .await
        .unwrap()
    }

    #[test]
    fn value_transfer() {
        run_test(async {
            let header = PartialHeader {
                number: 10_336_006.into(),
                ..PartialHeader::empty()
            };

            let sender = hex!("0a6bb546b9208cfab9e8fa2b9b2c042b18df7030").into();
            let to = hex!("8b299e2b7d7f43c0ce3068263545309ff4ffb521").into();
            let value = 10_200_000_000_000_000_u128;

            let mut db = InMemoryState::default();
            let mut state = IntraBlockState::new(&mut db);

            assert_eq!(state.get_balance(sender).await.unwrap(), 0);
            assert_eq!(state.get_balance(to).await.unwrap(), 0);

            let txn = MessageWithSender {
                message: Message::Legacy {
                    action: TransactionAction::Call(to),
                    value: value.into(),

                    chain_id: Default::default(),
                    nonce: Default::default(),
                    gas_price: Default::default(),
                    gas_limit: Default::default(),
                    input: Default::default(),
                },
                sender,
            };

            let gas = 0;

            let res = execute(&mut state, &header, &txn, gas).await;
            assert_eq!(res.status_code, StatusCode::InsufficientBalance);
            assert_eq!(res.output_data, vec![]);

            state.add_to_balance(sender, ETHER).await.unwrap();

            let res = execute(&mut state, &header, &txn, gas).await;
            assert_eq!(res.status_code, StatusCode::Success);
            assert_eq!(res.output_data, vec![]);

            assert_eq!(state.get_balance(sender).await.unwrap(), ETHER - value);
            assert_eq!(state.get_balance(to).await.unwrap(), value);
        })
    }

    #[test]
    fn smart_contract_with_storage() {
        run_test(async {
            let header = PartialHeader {
                number: 10_336_006.into(),
                ..PartialHeader::empty()
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

            let txn = |action, input| MessageWithSender {
                message: Message::Legacy {
                    input,
                    action,

                    chain_id: Default::default(),
                    nonce: Default::default(),
                    gas_price: Default::default(),
                    gas_limit: Default::default(),
                    value: Default::default(),
                },
                sender: caller,
            };

            let t = (txn)(TransactionAction::Create, code.to_vec().into());
            let gas = 0;
            let res = execute(&mut state, &header, &t, gas).await;
            assert_eq!(res.status_code, StatusCode::OutOfGas);
            assert_eq!(res.output_data, vec![]);

            let gas = 50_000;
            let res = execute(&mut state, &header, &t, gas).await;
            assert_eq!(res.status_code, StatusCode::Success);
            assert_eq!(res.output_data, bytes!("600035600055"));

            let contract_address = create_address(caller, 1);
            let key0 = 0.as_u256();
            assert_eq!(
                state
                    .get_current_storage(contract_address, key0)
                    .await
                    .unwrap(),
                0x2a
            );

            let new_val = 0xf5.as_u256();

            let res = execute(
                &mut state,
                &header,
                &(txn)(
                    TransactionAction::Call(contract_address),
                    u256_to_h256(new_val).0.to_vec().into(),
                ),
                gas,
            )
            .await;
            assert_eq!(res.status_code, StatusCode::Success);
            assert_eq!(res.output_data, vec![]);
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
            let header = PartialHeader {
                number: 1_431_916.into(),
                ..PartialHeader::empty()
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

            let txn = |input| MessageWithSender {
                sender: caller,
                message: Message::Legacy {
                    action: TransactionAction::Call(contract),
                    input,

                    chain_id: Default::default(),
                    nonce: Default::default(),
                    gas_price: Default::default(),
                    gas_limit: Default::default(),
                    value: Default::default(),
                },
            };

            let gas = 1_000_000;
            let res = execute(&mut state, &header, &(txn)(Default::default()), gas).await;
            assert_eq!(res.status_code, StatusCode::Success);
            assert_eq!(res.output_data, vec![]);

            let num_of_recursions = 0x0400;
            let res = execute(
                &mut state,
                &header,
                &(txn)(H256::from_low_u64_be(num_of_recursions).0.to_vec().into()),
                gas,
            )
            .await;
            assert_eq!(res.status_code, StatusCode::Success);
            assert_eq!(res.output_data, vec![]);

            let num_of_recursions = 0x0401;
            let res = execute(
                &mut state,
                &header,
                &(txn)(H256::from_low_u64_be(num_of_recursions).0.to_vec().into()),
                gas,
            )
            .await;
            assert_eq!(res.status_code, StatusCode::InvalidInstruction);
            assert_eq!(res.output_data, vec![]);
        })
    }

    #[test]
    fn delegatecall() {
        run_test(async {
            let header = PartialHeader {
                number: 1_639_560.into(),
                ..PartialHeader::empty()
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

            let txn = MessageWithSender {
                message: Message::Legacy {
                    action: TransactionAction::Call(caller_address),
                    input: H256::from(callee_address).0.to_vec().into(),

                    chain_id: Default::default(),
                    nonce: Default::default(),
                    gas_price: Default::default(),
                    gas_limit: Default::default(),
                    value: Default::default(),
                },
                sender: caller_address,
            };

            let gas = 1_000_000;
            let res = execute(&mut state, &header, &txn, gas).await;
            assert_eq!(res.status_code, StatusCode::Success);
            assert_eq!(res.output_data, vec![]);

            let key0 = 0.as_u256();
            assert_eq!(
                state
                    .get_current_storage(caller_address, key0)
                    .await
                    .unwrap(),
                h256_to_u256(H256::from(caller_address))
            );
        })
    }

    // https://eips.ethereum.org/EIPS/eip-211#specification
    #[test]
    fn create_should_only_return_on_failure() {
        run_test(async {
            let header = PartialHeader {
                number: 4_575_910.into(),
                ..PartialHeader::empty()
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

            let txn = MessageWithSender {
                message: Message::Legacy {
                    action: TransactionAction::Create,
                    input: code.to_vec().into(),

                    chain_id: Default::default(),
                    nonce: Default::default(),
                    gas_price: Default::default(),
                    gas_limit: Default::default(),
                    value: Default::default(),
                },
                sender: caller,
            };

            let gas = 150_000;
            let res = execute(&mut state, &header, &txn, gas).await;
            assert_eq!(res.status_code, StatusCode::Success);
            assert_eq!(res.output_data, vec![]);

            let contract_address = create_address(caller, 0);
            let key0 = 0.as_u256();
            assert_eq!(
                state
                    .get_current_storage(contract_address, key0)
                    .await
                    .unwrap(),
                0
            );
        })
    }

    // https://github.com/ethereum/EIPs/issues/684
    #[test]
    fn contract_overwrite() {
        run_test(async {
            let header = PartialHeader {
                number: 7_753_545.into(),
                ..PartialHeader::empty()
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

            let txn = MessageWithSender {
                message: Message::Legacy {
                    action: TransactionAction::Create,
                    input: new_code.to_vec().into(),

                    chain_id: Default::default(),
                    nonce: Default::default(),
                    gas_price: Default::default(),
                    gas_limit: Default::default(),
                    value: Default::default(),
                },
                sender: caller,
            };

            let gas = 100_000;
            let res = execute(&mut state, &header, &txn, gas).await;

            assert_eq!(res.status_code, StatusCode::InvalidInstruction);
            assert_eq!(res.gas_left, 0);
            assert_eq!(res.output_data, vec![]);
        })
    }

    #[test]
    fn eip3541() {
        run_test(async {
            let header = PartialHeader {
                number: 13_500_000.into(),
                ..PartialHeader::empty()
            };

            let mut db = InMemoryState::default();
            let mut state = IntraBlockState::new(&mut db);

            let t = |input| MessageWithSender {
                message: Message::Legacy {
                    action: TransactionAction::Create,
                    input,

                    chain_id: Default::default(),
                    nonce: Default::default(),
                    gas_price: Default::default(),
                    gas_limit: Default::default(),
                    value: Default::default(),
                },
                sender: hex!("1000000000000000000000000000000000000000").into(),
            };

            let gas = 50_000;

            // https://eips.ethereum.org/EIPS/eip-3541#test-cases
            let txn = (t)(hex!("60ef60005360016000f3").to_vec().into());
            assert_eq!(
                execute(&mut state, &header, &txn, gas).await.status_code,
                StatusCode::ContractValidationFailure
            );

            let txn = (t)(hex!("60ef60005360026000f3").to_vec().into());
            assert_eq!(
                execute(&mut state, &header, &txn, gas).await.status_code,
                StatusCode::ContractValidationFailure
            );

            let txn = (t)(hex!("60ef60005360036000f3").to_vec().into());
            assert_eq!(
                execute(&mut state, &header, &txn, gas).await.status_code,
                StatusCode::ContractValidationFailure
            );

            let txn = (t)(hex!("60ef60005360206000f3").to_vec().into());
            assert_eq!(
                execute(&mut state, &header, &txn, gas).await.status_code,
                StatusCode::ContractValidationFailure
            );

            let txn = (t)(hex!("60fe60005360016000f3").to_vec().into());
            assert_eq!(
                execute(&mut state, &header, &txn, gas).await.status_code,
                StatusCode::Success
            );
        })
    }
}
