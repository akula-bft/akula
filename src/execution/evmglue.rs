use super::{
    address::*,
    analysis_cache::AnalysisCache,
    precompiled,
    tracer::{CodeKind, MessageKind, Tracer},
};
use crate::{
    chain::protocol_param::{fee, param},
    crypto::keccak256,
    execution::evm::{
        host::*, AnalyzedCode, CallKind, CreateMessage, InterpreterMessage, Output, StatusCode,
    },
    h256_to_u256,
    models::*,
    u256_to_h256, HeaderReader, IntraBlockState, StateReader,
};
use anyhow::Context;
use bytes::Bytes;
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
    B: HeaderReader + StateReader,
{
    state: &'state mut IntraBlockState<'r, B>,
    tracer: &'tracer mut dyn Tracer,
    analysis_cache: &'analysis mut AnalysisCache,
    header: &'h BlockHeader,
    block_spec: &'c BlockExecutionSpec,
    message: &'t Message,
    sender: Address,
    beneficiary: Address,
}

pub fn execute<'db, 'tracer, 'analysis, B: HeaderReader + StateReader>(
    state: &mut IntraBlockState<'db, B>,
    tracer: &'tracer mut dyn Tracer,
    analysis_cache: &'analysis mut AnalysisCache,
    header: &BlockHeader,
    block_spec: &BlockExecutionSpec,
    message: &Message,
    sender: Address,
    beneficiary: Address,
    gas: u64,
) -> anyhow::Result<CallResult> {
    let mut evm = Evm {
        header,
        tracer,
        analysis_cache,
        state,
        block_spec,
        message,
        sender,
        beneficiary,
    };

    let res = if let TransactionAction::Call(to) = message.action() {
        evm.call(&InterpreterMessage {
            kind: CallKind::Call,
            is_static: false,
            depth: 0,
            sender,
            input_data: message.input().clone(),
            value: message.value(),
            gas: gas as i64,
            real_sender: sender,
            recipient: to,
            code_address: to,
        })?
    } else {
        evm.create(&CreateMessage {
            depth: 0,
            gas: gas as i64,
            sender,
            initcode: message.input().clone(),
            endowment: message.value(),
            salt: None,
        })?
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
    B: HeaderReader + StateReader,
{
    fn create(&mut self, message: &CreateMessage) -> anyhow::Result<Output> {
        let mut res = Output {
            status_code: StatusCode::Success,
            gas_left: message.gas,
            output_data: Bytes::new(),
            create_address: None,
        };

        let value = message.endowment;
        if self.state.get_balance(message.sender)? < value {
            res.status_code = StatusCode::InsufficientBalance;
            return Ok(res);
        }

        let nonce = self.state.get_nonce(message.sender)?;
        if let Some(new_nonce) = nonce.checked_add(1) {
            self.state.set_nonce(message.sender, new_nonce)?;
        } else {
            // EIP-2681: Limit account nonce to 2^64-1
            // See also https://github.com/ethereum/go-ethereum/blob/v1.10.13/core/vm/evm.go#L426
            res.status_code = StatusCode::ArgumentOutOfRange;
            return Ok(res);
        }

        let contract_addr = {
            if let Some(salt) = message.salt {
                create2_address(message.sender, salt, keccak256(&message.initcode[..]))
            } else {
                create_address(message.sender, nonce)
            }
        };

        self.state.access_account(contract_addr);

        self.tracer.capture_start(
            message.depth.try_into().unwrap(),
            message.sender,
            contract_addr,
            message.sender,
            contract_addr,
            MessageKind::Create { salt: message.salt },
            message.initcode.clone(),
            message.gas.try_into().unwrap(),
            message.endowment,
        );

        if self.state.get_nonce(contract_addr)? != 0
            || self.state.get_code_hash(contract_addr)? != EMPTY_HASH
        {
            // https://github.com/ethereum/EIPs/issues/684
            res.status_code = StatusCode::InvalidInstruction;
            res.gas_left = 0;
            return Ok(res);
        }

        let snapshot = self.state.take_snapshot();

        self.state.create_contract(contract_addr)?;

        if self.block_spec.revision >= Revision::Spurious {
            self.state.set_nonce(contract_addr, 1)?;
        }

        self.state.subtract_from_balance(message.sender, value)?;
        self.state.add_to_balance(contract_addr, value)?;

        let deploy_message = InterpreterMessage {
            kind: CallKind::Call,
            is_static: false,
            depth: message.depth,
            gas: message.gas,
            recipient: contract_addr,
            sender: message.sender,
            code_address: contract_addr,
            real_sender: message.sender,
            input_data: Default::default(),
            value: message.endowment,
        };

        res = self.execute(&deploy_message, &message.initcode, None)?;

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
                    .set_code(contract_addr, res.output_data.clone())?;
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

    fn call(&mut self, message: &InterpreterMessage) -> anyhow::Result<Output> {
        if message.kind != CallKind::DelegateCall
            && self.state.get_balance(message.sender)? < message.value
        {
            return Ok(Output {
                status_code: StatusCode::InsufficientBalance,
                gas_left: message.gas,
                output_data: Bytes::new(),
                create_address: None,
            });
        }

        let code_kind = if self.is_precompiled(message.code_address) {
            CodeKind::Precompile
        } else {
            CodeKind::Bytecode(self.state.get_code(message.code_address)?)
        };

        self.tracer.capture_start(
            message.depth as u16,
            message.sender,
            message.recipient,
            message.real_sender,
            message.code_address,
            MessageKind::Call {
                call_kind: match (message.kind, message.is_static) {
                    (CallKind::Call, true) => super::tracer::CallKind::StaticCall,
                    (CallKind::Call, false) => super::tracer::CallKind::Call,
                    (CallKind::CallCode, _) => super::tracer::CallKind::CallCode,
                    (CallKind::DelegateCall, _) => super::tracer::CallKind::DelegateCall,
                    _ => unreachable!(),
                },
                code_kind: code_kind.clone(),
            },
            message.input_data.clone(),
            message.gas as u64,
            message.value,
        );

        // https://eips.ethereum.org/EIPS/eip-161
        if message.value == 0
            && self.block_spec.revision >= Revision::Spurious
            && matches!(code_kind, CodeKind::Bytecode(_))
            && !self.state.exists(message.code_address)?
        {
            return Ok(Output {
                status_code: StatusCode::Success,
                gas_left: message.gas,
                output_data: Bytes::new(),
                create_address: None,
            });
        }

        let snapshot = self.state.take_snapshot();

        if message.kind == CallKind::Call {
            if message.is_static {
                // Match geth logic
                // https://github.com/ethereum/go-ethereum/blob/v1.9.25/core/vm/evm.go#L391
                self.state.touch(message.recipient);
            } else {
                self.state
                    .subtract_from_balance(message.sender, message.value)?;
                self.state
                    .add_to_balance(message.recipient, message.value)?;
            }
        }

        let mut res = match &code_kind {
            CodeKind::Bytecode(code) => {
                let code = code.as_ref().map(|v| v as &[u8]).unwrap_or(&[]);
                if code.is_empty() {
                    return Ok(Output {
                        status_code: StatusCode::Success,
                        gas_left: message.gas,
                        output_data: Bytes::new(),
                        create_address: None,
                    });
                }

                let code_hash = self.state.get_code_hash(message.code_address)?;

                self.execute(message, code, Some(&code_hash))?
            }
            CodeKind::Precompile => {
                let num = message.code_address.0[ADDRESS_LENGTH - 1] as u8;
                let contract = precompiled::CONTRACTS.get(&num).unwrap();
                let input = message.input_data.clone();
                let mut res = Output {
                    status_code: StatusCode::Success,
                    gas_left: message.gas,
                    output_data: Bytes::new(),
                    create_address: None,
                };
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
                res
            }
        };

        if res.status_code != StatusCode::Success {
            self.state.revert_to_snapshot(snapshot);
            if res.status_code != StatusCode::Revert {
                res.gas_left = 0;
            }
        }

        Ok(res)
    }

    fn execute(
        &mut self,
        msg: &InterpreterMessage,
        code: &[u8],
        code_hash: Option<&H256>,
    ) -> anyhow::Result<Output> {
        let analysis = if let Some(code_hash) = code_hash {
            if let Some(cache) = self.analysis_cache.get(code_hash).cloned() {
                cache
            } else {
                let analysis = AnalyzedCode::analyze(code);
                self.analysis_cache.put(*code_hash, analysis.clone());
                analysis
            }
        } else {
            AnalyzedCode::analyze(code)
        };

        let revision = self.block_spec.revision;

        let output = analysis.execute(self, msg, revision);

        self.tracer.capture_end(
            msg.depth.try_into().unwrap(),
            msg.gas.try_into().unwrap(),
            &output,
        );

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
            Revision::Istanbul | Revision::Berlin | Revision::London | Revision::Paris => {
                precompiled::NUM_OF_ISTANBUL_CONTRACTS as u8
            }
        }
    }

    fn number_of_parlia_precompiles(&self) -> u8 {
        match self.block_spec.revision {
            Revision::Frontier | Revision::Homestead | Revision::Tangerine | Revision::Spurious => {
                precompiled::NUM_OF_FRONTIER_CONTRACTS as u8
            }
            Revision::Byzantium | Revision::Constantinople | Revision::Petersburg => {
                precompiled::NUM_OF_BYZANTIUM_CONTRACTS as u8
            }
            Revision::Istanbul | Revision::Berlin | Revision::London | Revision::Paris => {
                precompiled::NUM_OF_PARLIA_ISTANBUL_CONTRACTS as u8
            }
        }
    }

    fn is_precompiled(&self, contract: Address) -> bool {
        if contract.is_zero() {
            false
        } else if self.block_spec.consensus.is_parlia() {
            let mut max_precompiled = Address::zero();
            let num_of_precompiled = self.number_of_parlia_precompiles();
            if num_of_precompiled < precompiled::NUM_OF_PARLIA_ISTANBUL_CONTRACTS as u8 {
                max_precompiled.0[ADDRESS_LENGTH - 1] = num_of_precompiled;
            } else {
                max_precompiled.0[ADDRESS_LENGTH - 1] = precompiled::MAX_NUM_OF_PARLIA_PRECOMPILED as u8;
            }

            let num_of_contract= contract.0[ADDRESS_LENGTH -1] as u8;
            contract <= max_precompiled && precompiled::CONTRACTS.contains_key(&num_of_contract)
        } else {
            let mut max_precompiled = Address::zero();
            max_precompiled.0[ADDRESS_LENGTH - 1] = self.number_of_precompiles() as u8;
            contract <= max_precompiled
        }
    }
}

impl<'r, 'state, 'tracer, 'analysis, 'h, 'c, 't, B: HeaderReader + StateReader> Host
    for Evm<'r, 'state, 'tracer, 'analysis, 'h, 'c, 't, B>
{
    fn trace_instructions(&self) -> bool {
        self.tracer.trace_instructions()
    }
    fn tracer(&mut self, mut f: impl FnMut(&mut dyn Tracer)) {
        (f)(self.tracer)
    }

    fn account_exists(&mut self, address: Address) -> bool {
        if self.block_spec.revision >= Revision::Spurious {
            !self.state.is_dead(address).unwrap()
        } else {
            self.state.exists(address).unwrap()
        }
    }

    fn get_storage(&mut self, address: Address, location: U256) -> U256 {
        self.state.get_current_storage(address, location).unwrap()
    }

    fn set_storage(&mut self, address: Address, location: U256, new_val: U256) -> StorageStatus {
        let current_val = self.state.get_current_storage(address, location).unwrap();

        if current_val == new_val {
            StorageStatus::Unchanged
        } else {
            self.state.set_storage(address, location, new_val).unwrap();

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
                let original_val = self.state.get_original_storage(address, location).unwrap();

                // https://eips.ethereum.org/EIPS/eip-3529
                let sstore_clears_refund = if self.block_spec.revision >= Revision::London {
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
        }
    }

    fn get_balance(&mut self, address: Address) -> U256 {
        self.state.get_balance(address).unwrap()
    }

    fn get_code_size(&mut self, address: Address) -> U256 {
        u64::try_from(
            self.state
                .get_code(address)
                .unwrap()
                .map(|c| c.len())
                .unwrap_or(0),
        )
        .unwrap()
        .into()
    }

    fn get_code_hash(&mut self, address: Address) -> U256 {
        h256_to_u256({
            if self.state.is_dead(address).unwrap() {
                H256::zero()
            } else {
                self.state.get_code_hash(address).unwrap()
            }
        })
    }

    fn copy_code(&mut self, address: Address, offset: usize, buffer: &mut [u8]) -> usize {
        let code = self.state.get_code(address).unwrap().unwrap_or_default();

        let mut copied = 0;
        if offset < code.len() {
            copied = min(buffer.len(), code.len() - offset);
            buffer[..copied].copy_from_slice(&code[offset..offset + copied]);
        }

        copied
    }

    fn selfdestruct(&mut self, address: Address, beneficiary: Address) {
        self.state.record_selfdestruct(address);
        let balance = self.state.get_balance(address).unwrap();
        self.state.add_to_balance(beneficiary, balance).unwrap();
        self.state.set_balance(address, 0).unwrap();

        self.tracer(|t| t.capture_self_destruct(address, beneficiary, balance));
    }

    fn call(&mut self, msg: Call) -> Output {
        match msg {
            Call::Create(message) => {
                let mut res = self.create(message).unwrap();

                // https://eips.ethereum.org/EIPS/eip-211
                if res.status_code != StatusCode::Revert {
                    // geth returns CREATE output only in case of REVERT
                    res.output_data = Default::default();
                }

                res
            }
            Call::Call(message) => self.call(message).unwrap(),
        }
    }

    fn get_tx_context(&mut self) -> Result<TxContext, StatusCode> {
        let base_fee_per_gas = self.header.base_fee_per_gas.unwrap_or(U256::ZERO);
        let tx_gas_price = self
            .message
            .effective_gas_price(base_fee_per_gas)
            .ok_or(StatusCode::InternalError("tx gas price too low"))?;
        let tx_origin = self.sender;
        let block_coinbase = self.beneficiary;
        let block_number = self.header.number.0;
        let block_timestamp = self.header.timestamp;
        let block_gas_limit = self.header.gas_limit;
        let block_difficulty = if self.block_spec.revision >= Revision::Paris {
            h256_to_u256(self.header.mix_hash)
        } else {
            self.header.difficulty
        };
        let chain_id = self.block_spec.params.chain_id.0.into();
        let block_base_fee = base_fee_per_gas;

        Ok(TxContext {
            tx_gas_price,
            tx_origin,
            block_coinbase,
            block_number,
            block_timestamp,
            block_gas_limit,
            block_difficulty,
            chain_id,
            block_base_fee,
        })
    }

    fn get_block_hash(&mut self, block_number: u64) -> U256 {
        let base_number = self.header.number;
        let distance = base_number.0 - block_number;
        assert!(distance <= 256);

        let mut hash = self.header.parent_hash;

        for i in 1..distance {
            hash = self
                .state
                .db()
                .read_header(BlockNumber(base_number.0 - i), hash)
                .unwrap()
                .context("no header")
                .unwrap()
                .parent_hash;
        }

        h256_to_u256(hash)
    }

    fn emit_log(&mut self, address: Address, data: Bytes, topics: &[U256]) {
        self.state.add_log(Log {
            address,
            topics: topics.iter().copied().map(u256_to_h256).collect(),
            data,
        });
    }

    fn access_account(&mut self, address: Address) -> AccessStatus {
        if self.is_precompiled(address) {
            AccessStatus::Warm
        } else {
            self.state.access_account(address)
        }
    }

    fn access_storage(&mut self, address: Address, location: U256) -> AccessStatus {
        self.state.access_storage(address, location)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{execution::tracer::NoopTracer, res::chainspec::MAINNET, InMemoryState};
    use bytes_literal::bytes;
    use hex_literal::hex;

    fn execute<B: HeaderReader + StateReader>(
        state: &mut IntraBlockState<'_, B>,
        header: &PartialHeader,
        message: &Message,
        sender: Address,
        gas: u64,
    ) -> CallResult {
        let mut tracer = NoopTracer;
        let beneficiary = header.beneficiary;
        let header = BlockHeader::new(header.clone(), EMPTY_LIST_HASH, EMPTY_ROOT);
        super::execute(
            state,
            &mut tracer,
            &mut AnalysisCache::default(),
            &header,
            &MAINNET.collect_block_spec(header.number),
            message,
            sender,
            beneficiary,
            gas,
        )
        .unwrap()
    }

    #[test]
    fn value_transfer() {
        let header = PartialHeader {
            number: 10_336_006.into(),
            ..PartialHeader::empty()
        };

        let sender = hex!("0a6bb546b9208cfab9e8fa2b9b2c042b18df7030").into();
        let to = hex!("8b299e2b7d7f43c0ce3068263545309ff4ffb521").into();
        let value = 10_200_000_000_000_000_u128;

        let mut db = InMemoryState::default();
        let mut state = IntraBlockState::new(&mut db);

        assert_eq!(state.get_balance(sender).unwrap(), 0);
        assert_eq!(state.get_balance(to).unwrap(), 0);

        let message = Message::Legacy {
            action: TransactionAction::Call(to),
            value: value.into(),

            chain_id: Default::default(),
            nonce: Default::default(),
            gas_price: Default::default(),
            gas_limit: Default::default(),
            input: Default::default(),
        };

        let gas = 0;

        let res = execute(&mut state, &header, &message, sender, gas);
        assert_eq!(res.status_code, StatusCode::InsufficientBalance);
        assert_eq!(res.output_data, vec![]);

        state.add_to_balance(sender, ETHER).unwrap();

        let res = execute(&mut state, &header, &message, sender, gas);
        assert_eq!(res.status_code, StatusCode::Success);
        assert_eq!(res.output_data, vec![]);

        assert_eq!(state.get_balance(sender).unwrap(), ETHER - value);
        assert_eq!(state.get_balance(to).unwrap(), value);
    }

    #[test]
    fn smart_contract_with_storage() {
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

        let m = |action, input| Message::Legacy {
            input,
            action,

            chain_id: Default::default(),
            nonce: Default::default(),
            gas_price: Default::default(),
            gas_limit: Default::default(),
            value: Default::default(),
        };

        let message = (m)(TransactionAction::Create, code.to_vec().into());
        let gas = 0;
        let res = execute(&mut state, &header, &message, caller, gas);
        assert_eq!(res.status_code, StatusCode::OutOfGas);
        assert_eq!(res.output_data, vec![]);

        let gas = 50_000;
        let res = execute(&mut state, &header, &message, caller, gas);
        assert_eq!(res.status_code, StatusCode::Success);
        assert_eq!(res.output_data, bytes!("600035600055"));

        let contract_address = create_address(caller, 1);
        let key0 = 0.as_u256();
        assert_eq!(
            state.get_current_storage(contract_address, key0).unwrap(),
            0x2a
        );

        let new_val = 0xf5.as_u256();

        let res = execute(
            &mut state,
            &header,
            &(m)(
                TransactionAction::Call(contract_address),
                u256_to_h256(new_val).0.to_vec().into(),
            ),
            caller,
            gas,
        );
        assert_eq!(res.status_code, StatusCode::Success);
        assert_eq!(res.output_data, vec![]);
        assert_eq!(
            state.get_current_storage(contract_address, key0).unwrap(),
            new_val
        );
    }

    #[test]
    fn maximum_call_depth() {
        std::thread::Builder::new()
            .stack_size(128 * 1024 * 1024)
            .spawn(move || {
                let header = PartialHeader {
                    number: 1_431_916.into(),
                    ..PartialHeader::empty()
                };
                let caller = hex!("8e4d1ea201b908ab5e1f5a1c3f9f1b4f6c1e9cf1").into();
                let contract = hex!("3589d05a1ec4af9f65b0e5554e645707775ee43c").into();

                // The contract just calls itself recursively a given number of times.
                let code = hex!(
                    "60003580600857005b6001900360005260008060208180305a6103009003f1602357fe5b"
                );

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

                state.set_code(contract, code.to_vec().into()).unwrap();

                let m = |input| Message::Legacy {
                    action: TransactionAction::Call(contract),
                    input,

                    chain_id: Default::default(),
                    nonce: Default::default(),
                    gas_price: Default::default(),
                    gas_limit: Default::default(),
                    value: Default::default(),
                };

                let gas = 1_000_000;
                let res = execute(&mut state, &header, &(m)(Default::default()), caller, gas);
                assert_eq!(res.status_code, StatusCode::Success);
                assert_eq!(res.output_data, vec![]);

                let num_of_recursions = 0x0400;
                let res = execute(
                    &mut state,
                    &header,
                    &(m)(H256::from_low_u64_be(num_of_recursions).0.to_vec().into()),
                    caller,
                    gas,
                );
                assert_eq!(res.status_code, StatusCode::Success);
                assert_eq!(res.output_data, vec![]);

                let num_of_recursions = 0x0401;
                let res = execute(
                    &mut state,
                    &header,
                    &(m)(H256::from_low_u64_be(num_of_recursions).0.to_vec().into()),
                    caller,
                    gas,
                );
                assert_eq!(res.status_code, StatusCode::InvalidInstruction);
                assert_eq!(res.output_data, vec![]);
            })
            .unwrap()
            .join()
            .unwrap()
    }

    #[test]
    fn delegatecall() {
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
            .unwrap();
        state
            .set_code(callee_address, callee_code.to_vec().into())
            .unwrap();

        let message = Message::Legacy {
            action: TransactionAction::Call(caller_address),
            input: H256::from(callee_address).0.to_vec().into(),

            chain_id: Default::default(),
            nonce: Default::default(),
            gas_price: Default::default(),
            gas_limit: Default::default(),
            value: Default::default(),
        };

        let gas = 1_000_000;
        let res = execute(&mut state, &header, &message, caller_address, gas);
        assert_eq!(res.status_code, StatusCode::Success);
        assert_eq!(res.output_data, vec![]);

        let key0 = 0.as_u256();
        assert_eq!(
            state.get_current_storage(caller_address, key0).unwrap(),
            h256_to_u256(H256::from(caller_address))
        );
    }

    // https://eips.ethereum.org/EIPS/eip-211#specification
    #[test]
    fn create_should_only_return_on_failure() {
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

        let message = Message::Legacy {
            action: TransactionAction::Create,
            input: code.to_vec().into(),

            chain_id: Default::default(),
            nonce: Default::default(),
            gas_price: Default::default(),
            gas_limit: Default::default(),
            value: Default::default(),
        };

        let gas = 150_000;
        let res = execute(&mut state, &header, &message, caller, gas);
        assert_eq!(res.status_code, StatusCode::Success);
        assert_eq!(res.output_data, vec![]);

        let contract_address = create_address(caller, 0);
        let key0 = 0.as_u256();
        assert_eq!(
            state.get_current_storage(contract_address, key0).unwrap(),
            0
        );
    }

    // https://github.com/ethereum/EIPs/issues/684
    #[test]
    fn contract_overwrite() {
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
            .unwrap();

        let message = Message::Legacy {
            action: TransactionAction::Create,
            input: new_code.to_vec().into(),

            chain_id: Default::default(),
            nonce: Default::default(),
            gas_price: Default::default(),
            gas_limit: Default::default(),
            value: Default::default(),
        };

        let gas = 100_000;
        let res = execute(&mut state, &header, &message, caller, gas);

        assert_eq!(res.status_code, StatusCode::InvalidInstruction);
        assert_eq!(res.gas_left, 0);
        assert_eq!(res.output_data, vec![]);
    }

    #[test]
    fn eip3541() {
        let header = PartialHeader {
            number: 13_500_000.into(),
            ..PartialHeader::empty()
        };

        let mut db = InMemoryState::default();
        let mut state = IntraBlockState::new(&mut db);

        let sender = hex!("1000000000000000000000000000000000000000").into();
        let m = |input| Message::Legacy {
            action: TransactionAction::Create,
            input,

            chain_id: Default::default(),
            nonce: Default::default(),
            gas_price: Default::default(),
            gas_limit: Default::default(),
            value: Default::default(),
        };

        let gas = 50_000;

        // https://eips.ethereum.org/EIPS/eip-3541#test-cases
        let message = (m)(hex!("60ef60005360016000f3").to_vec().into());
        assert_eq!(
            execute(&mut state, &header, &message, sender, gas).status_code,
            StatusCode::ContractValidationFailure
        );

        let message = (m)(hex!("60ef60005360026000f3").to_vec().into());
        assert_eq!(
            execute(&mut state, &header, &message, sender, gas).status_code,
            StatusCode::ContractValidationFailure
        );

        let message = (m)(hex!("60ef60005360036000f3").to_vec().into());
        assert_eq!(
            execute(&mut state, &header, &message, sender, gas).status_code,
            StatusCode::ContractValidationFailure
        );

        let message = (m)(hex!("60ef60005360206000f3").to_vec().into());
        assert_eq!(
            execute(&mut state, &header, &message, sender, gas).status_code,
            StatusCode::ContractValidationFailure
        );

        let message = (m)(hex!("60fe60005360016000f3").to_vec().into());
        assert_eq!(
            execute(&mut state, &header, &message, sender, gas).status_code,
            StatusCode::Success
        );
    }
}
