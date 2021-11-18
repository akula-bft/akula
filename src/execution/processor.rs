use super::{analysis_cache::AnalysisCache, root_hash};
use crate::{
    chain::{
        intrinsic_gas::*,
        protocol_param::{fee, param},
    },
    consensus::*,
    execution::evm,
    models::*,
    state::IntraBlockState,
    State,
};
use anyhow::Context;
use ethereum_types::*;
use evmodin::{Revision, StatusCode};
use std::cmp::min;
use TransactionAction;

pub struct ExecutionProcessor<'r, 'analysis, 'e, 'h, 'b, 'c, S>
where
    S: State,
{
    state: IntraBlockState<'r, S>,
    analysis_cache: &'analysis mut AnalysisCache,
    engine: &'e mut dyn Consensus,
    header: &'h PartialHeader,
    block: &'b BlockBodyWithSenders,
    block_spec: &'c BlockExecutionSpec,
    cumulative_gas_used: u64,
}

impl<'r, 'analysis, 'e, 'h, 'b, 'c, S> ExecutionProcessor<'r, 'analysis, 'e, 'h, 'b, 'c, S>
where
    S: State,
{
    pub fn new(
        state: &'r mut S,
        analysis_cache: &'analysis mut AnalysisCache,
        engine: &'e mut dyn Consensus,
        header: &'h PartialHeader,
        block: &'b BlockBodyWithSenders,
        block_spec: &'c BlockExecutionSpec,
    ) -> Self {
        Self {
            state: IntraBlockState::new(state),
            analysis_cache,
            engine,
            header,
            block,
            block_spec,
            cumulative_gas_used: 0,
        }
    }

    fn available_gas(&self) -> u64 {
        self.header.gas_limit - self.cumulative_gas_used
    }

    pub(crate) fn state(&mut self) -> &mut IntraBlockState<'r, S> {
        &mut self.state
    }

    pub(crate) fn into_state(self) -> IntraBlockState<'r, S> {
        self.state
    }

    pub async fn validate_transaction(&mut self, tx: &TransactionWithSender) -> anyhow::Result<()> {
        pre_validate_transaction(
            tx,
            self.block_spec.params.chain_id,
            self.header.base_fee_per_gas,
        )
        .expect("Tx must have been prevalidated");

        if self.state.get_code_hash(tx.sender).await? != EMPTY_HASH {
            return Err(ValidationError::SenderNoEOA { sender: tx.sender }.into());
        }

        let expected_nonce = self.state.get_nonce(tx.sender).await?;
        if expected_nonce != tx.nonce() {
            return Err(ValidationError::WrongNonce {
                account: tx.sender,
                expected: expected_nonce,
                got: tx.nonce(),
            }
            .into());
        }

        // https://github.com/ethereum/EIPs/pull/3594
        let max_gas_cost = U512::from(tx.gas_limit()) * U512::from(tx.max_fee_per_gas());
        // See YP, Eq (57) in Section 6.2 "Execution"
        let v0 = max_gas_cost + tx.value();
        let available_balance = U512::from(self.state.get_balance(tx.sender).await?);
        if available_balance < v0 {
            return Err(ValidationError::InsufficientFunds {
                account: tx.sender,
                available: available_balance,
                required: v0,
            }
            .into());
        }

        let available_gas = self.available_gas();
        if available_gas < tx.gas_limit() {
            // Corresponds to the final condition of Eq (58) in Yellow Paper Section 6.2 "Execution".
            // The sum of the transaction’s gas limit and the gas utilized in this block prior
            // must be no greater than the block’s gas limit.
            return Err(ValidationError::BlockGasLimitExceeded {
                available: available_gas,
                required: tx.gas_limit(),
            }
            .into());
        }

        Ok(())
    }

    async fn execute_transaction(
        &mut self,
        txn: &TransactionWithSender,
    ) -> anyhow::Result<Receipt> {
        let rev = self.block_spec.revision;

        self.state.clear_journal_and_substate();

        self.state.access_account(txn.sender);

        let base_fee_per_gas = self.header.base_fee_per_gas.unwrap_or_else(U256::zero);
        let effective_gas_price = txn.effective_gas_price(base_fee_per_gas);
        self.state
            .subtract_from_balance(
                txn.sender,
                U256::from(txn.gas_limit()) * effective_gas_price,
            )
            .await?;

        if let TransactionAction::Call(to) = txn.action() {
            self.state.access_account(to);
            // EVM itself increments the nonce for contract creation
            self.state.set_nonce(txn.sender, txn.nonce() + 1).await?;
        }

        for entry in &*txn.access_list() {
            self.state.access_account(entry.address);
            for &key in &entry.slots {
                self.state.access_storage(entry.address, key);
            }
        }

        let g0 = intrinsic_gas(txn, rev >= Revision::Homestead, rev >= Revision::Istanbul);
        let gas = u128::from(txn.gas_limit())
            .checked_sub(g0)
            .ok_or(ValidationError::IntrinsicGas)? as u64;

        let vm_res = evm::execute(
            &mut self.state,
            self.analysis_cache,
            self.header,
            self.block_spec,
            txn,
            gas,
        )
        .await?;

        let gas_used = txn.gas_limit() - self.refund_gas(txn, vm_res.gas_left as u64).await?;

        // award the miner
        let priority_fee_per_gas = txn.priority_fee_per_gas(base_fee_per_gas);
        self.state
            .add_to_balance(
                self.header.beneficiary,
                U256::from(gas_used) * priority_fee_per_gas,
            )
            .await?;

        self.state.destruct_selfdestructs().await?;
        if rev >= Revision::Spurious {
            self.state.destruct_touched_dead().await?;
        }

        self.state.finalize_transaction();

        self.cumulative_gas_used += gas_used;

        Ok(Receipt {
            tx_type: txn.tx_type(),
            success: vm_res.status_code == StatusCode::Success,
            cumulative_gas_used: self.cumulative_gas_used,
            bloom: logs_bloom(self.state.logs()),
            logs: self.state.logs().to_vec(),
        })
    }

    pub async fn execute_block_no_post_validation(&mut self) -> anyhow::Result<Vec<Receipt>> {
        let mut receipts = Vec::with_capacity(self.block.transactions.len());

        for (&address, &balance) in &self.block_spec.balance_changes {
            self.state.set_balance(address, balance).await?;
        }

        for (i, txn) in self.block.transactions.iter().enumerate() {
            self.validate_transaction(txn)
                .await
                .with_context(|| format!("Failed to validate tx #{}", i))?;
            receipts.push(self.execute_transaction(txn).await?);
        }

        for change in self
            .engine
            .finalize(self.header, &self.block.ommers, self.block_spec.revision)
            .await?
        {
            match change {
                FinalizationChange::Reward { address, amount } => {
                    self.state.add_to_balance(address, amount).await?;
                }
            }
        }

        Ok(receipts)
    }

    pub async fn execute_and_write_block(mut self) -> anyhow::Result<Vec<Receipt>> {
        let receipts = self.execute_block_no_post_validation().await?;

        let gas_used = receipts.last().map(|r| r.cumulative_gas_used).unwrap_or(0);

        if gas_used != self.header.gas_used {
            let transactions = receipts
                .into_iter()
                .enumerate()
                .fold(
                    (Vec::new(), 0),
                    |(mut receipts, last_gas_used), (i, receipt)| {
                        let gas_used = receipt.cumulative_gas_used - last_gas_used;
                        receipts.push((i, gas_used));
                        (receipts, receipt.cumulative_gas_used)
                    },
                )
                .0;
            return Err(ValidationError::WrongBlockGas {
                expected: self.header.gas_used,
                got: gas_used,
                transactions,
            }
            .into());
        }

        let block_num = self.header.number;
        let rev = self.block_spec.revision;

        if rev >= Revision::Byzantium {
            let expected = root_hash(&receipts);
            if expected != self.header.receipts_root {
                return Err(ValidationError::WrongReceiptsRoot {
                    expected,
                    got: self.header.receipts_root,
                }
                .into());
            }
        }

        let expected_logs_bloom = receipts
            .iter()
            .fold(Bloom::zero(), |bloom, r| bloom | r.bloom);
        if expected_logs_bloom != self.header.logs_bloom {
            return Err(ValidationError::WrongLogsBloom {
                expected: expected_logs_bloom,
                got: self.header.logs_bloom,
            }
            .into());
        }

        self.state.write_to_db(block_num).await?;

        Ok(receipts)
    }

    async fn refund_gas(
        &mut self,
        txn: &TransactionWithSender,
        mut gas_left: u64,
    ) -> anyhow::Result<u64> {
        let mut refund = self.state.get_refund();
        if self.block_spec.revision < Revision::London {
            refund += fee::R_SELF_DESTRUCT * self.state.number_of_self_destructs() as u64;
        }
        let max_refund_quotient = if self.block_spec.revision >= Revision::London {
            param::MAX_REFUND_QUOTIENT_LONDON
        } else {
            param::MAX_REFUND_QUOTIENT_FRONTIER
        };
        let max_refund = (txn.gas_limit() - gas_left) / max_refund_quotient;
        refund = min(refund, max_refund);
        gas_left += refund;

        let base_fee_per_gas = self.header.base_fee_per_gas.unwrap_or_else(U256::zero);
        let effective_gas_price = txn.effective_gas_price(base_fee_per_gas);
        self.state
            .add_to_balance(txn.sender, U256::from(gas_left) * effective_gas_price)
            .await?;

        Ok(gas_left)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        execution::address::create_address, res::chainspec::MAINNET, util::test_util::run_test,
        InMemoryState,
    };
    use bytes::Bytes;
    use bytes_literal::bytes;
    use hex_literal::hex;

    #[test]
    fn zero_gas_price() {
        run_test(async {
            let header = PartialHeader {
                number: 2_687_232.into(),
                gas_limit: 3_303_221,
                beneficiary: hex!("4bb96091ee9d802ed039c4d1a5f6216f90f81b01").into(),
                ..PartialHeader::empty()
            };
            let block = Default::default();

            // The sender does not exist
            let sender = hex!("004512399a230565b99be5c3b0030a56f3ace68c").into();

            let txn = TransactionWithSender {
                message: TransactionMessage::Legacy {
                    chain_id: None,
                    nonce: 0,
                    gas_price: U256::zero(),
                    gas_limit: 764_017,
                    action: TransactionAction::Create,
                    value: U256::zero(),
                    input: hex!("606060").to_vec().into(),
                },
                sender,
            };

            let mut state = InMemoryState::default();
            let mut analysis_cache = AnalysisCache::default();
            let mut engine = engine_factory(MAINNET.clone()).unwrap();
            let block_spec = MAINNET.collect_block_spec(header.number);
            let mut processor = ExecutionProcessor::new(
                &mut state,
                &mut analysis_cache,
                &mut *engine,
                &header,
                &block,
                &block_spec,
            );

            let receipt = processor.execute_transaction(&txn).await.unwrap();
            assert!(receipt.success);
        })
    }

    #[test]
    fn eip3607_reject_transactions_from_senders_with_deployed_code() {
        run_test(async {
            let header = PartialHeader {
                number: 1.into(),
                gas_limit: 3_000_000,
                ..PartialHeader::empty()
            };

            let sender = hex!("71562b71999873DB5b286dF957af199Ec94617F7").into();

            let tx = TransactionWithSender {
                sender,
                message: TransactionMessage::Legacy {
                    chain_id: None,
                    nonce: 0,
                    gas_price: U256::from(50) * GIGA,
                    gas_limit: 90_000,
                    action: TransactionAction::Call(
                        hex!("e5ef458d37212a06e3f59d40c454e76150ae7c32").into(),
                    ),
                    value: U256::from(1_027_501_080) * U256::from(GIGA),
                    input: Bytes::new(),
                },
            };

            let block = Default::default();

            let mut state = InMemoryState::default();
            let mut analysis_cache = AnalysisCache::default();
            let mut engine = engine_factory(MAINNET.clone()).unwrap();
            let block_spec = MAINNET.collect_block_spec(header.number);
            let mut processor = ExecutionProcessor::new(
                &mut state,
                &mut analysis_cache,
                &mut *engine,
                &header,
                &block,
                &block_spec,
            );

            processor
                .state
                .add_to_balance(sender, U256::from(10) * *ETHER)
                .await
                .unwrap();
            processor
                .state
                .set_code(sender, bytes!("B0B0FACE"))
                .await
                .unwrap();

            processor.validate_transaction(&tx).await.unwrap_err();
        })
    }

    #[test]
    fn no_refund_on_error() {
        run_test(async {
            let header = PartialHeader {
                number: 10_050_107.into(),
                gas_limit: 328_646,
                beneficiary: hex!("5146556427ff689250ed1801a783d12138c3dd5e").into(),
                ..PartialHeader::empty()
            };
            let block = Default::default();
            let caller = hex!("834e9b529ac9fa63b39a06f8d8c9b0d6791fa5df").into();
            let nonce = 3;

            // This contract initially sets its 0th storage to 0x2a.
            // When called, it updates the 0th storage to the input provided.
            let code = hex!("602a60005560098060106000396000f36000358060005531");
            // https://github.com/CoinCulture/evm-tools
            // 0      PUSH1  => 2a
            // 2      PUSH1  => 00
            // 4      SSTORE
            // 5      PUSH1  => 09
            // 7      DUP1
            // 8      PUSH1  => 10
            // 10     PUSH1  => 00
            // 12     CODECOPY
            // 13     PUSH1  => 00
            // 15     RETURN
            // ---------------------------
            // 16     PUSH1  => 00
            // 18     CALLDATALOAD
            // 19     DUP1
            // 20     PUSH1  => 00
            // 22     SSTORE
            // 23     BALANCE

            let mut state = InMemoryState::default();
            let mut analysis_cache = AnalysisCache::default();
            let mut engine = engine_factory(MAINNET.clone()).unwrap();
            let block_spec = MAINNET.collect_block_spec(header.number);
            let mut processor = ExecutionProcessor::new(
                &mut state,
                &mut analysis_cache,
                &mut *engine,
                &header,
                &block,
                &block_spec,
            );

            let t = |action, input, nonce, gas_limit| TransactionWithSender {
                message: TransactionMessage::EIP1559 {
                    chain_id: MAINNET.params.chain_id,
                    nonce,
                    max_priority_fee_per_gas: U256::zero(),
                    max_fee_per_gas: U256::from(59 * GIGA),
                    gas_limit,
                    action,
                    value: U256::zero(),
                    input,
                    access_list: Default::default(),
                },
                sender: caller,
            };

            let txn = (t)(
                TransactionAction::Create,
                code.to_vec().into(),
                nonce,
                103_858,
            );

            processor
                .state()
                .add_to_balance(caller, *ETHER)
                .await
                .unwrap();
            processor.state().set_nonce(caller, nonce).await.unwrap();

            let receipt1 = processor.execute_transaction(&txn).await.unwrap();
            assert!(receipt1.success);

            // Call the newly created contract
            // It should run SSTORE(0,0) with a potential refund
            // But then there's not enough gas for the BALANCE operation
            let txn = (t)(
                TransactionAction::Call(create_address(caller, nonce)),
                vec![].into(),
                nonce + 1,
                fee::G_TRANSACTION + 5_020,
            );

            let receipt2 = processor.execute_transaction(&txn).await.unwrap();
            assert!(!receipt2.success);
            assert_eq!(
                receipt2.cumulative_gas_used - receipt1.cumulative_gas_used,
                txn.gas_limit()
            );
        })
    }

    #[test]
    fn selfdestruct() {
        run_test(async {
            let header = PartialHeader {
                number: 1_487_375.into(),
                gas_limit: 4_712_388,
                beneficiary: hex!("61c808d82a3ac53231750dadc13c777b59310bd9").into(),
                ..PartialHeader::empty()
            };
            let block = Default::default();
            let suicidal_address = hex!("6d20c1c07e56b7098eb8c50ee03ba0f6f498a91d").into();
            let caller_address = hex!("4bf2054ffae7a454a35fd8cf4be21b23b1f25a6f").into();
            let originator = hex!("5a0b54d5dc17e0aadc383d2db43b0a0d3e029c4c").into();

            // The contract self-destructs if called with zero value.
            let suicidal_code = hex!("346007576000ff5b");
            // https://github.com/CoinCulture/evm-tools
            // 0      CALLVALUE
            // 1      PUSH1  => 07
            // 3      JUMPI
            // 4      PUSH1  => 00
            // 6      SUICIDE
            // 7      JUMPDEST

            // The caller calls the input contract three times:
            // twice with zero value and once with non-zero value.
            let caller_code = hex!(
                "600080808080803561eeeef150600080808080803561eeeef15060008080806005813561eeeef1"
            );
            // https://github.com/CoinCulture/evm-tools
            // 0      PUSH1  => 00
            // 2      DUP1
            // 3      DUP1
            // 4      DUP1
            // 5      DUP1
            // 6      DUP1
            // 7      CALLDATALOAD
            // 8      PUSH2  => eeee
            // 11     CALL
            // 12     POP
            // 13     PUSH1  => 00
            // 15     DUP1
            // 16     DUP1
            // 17     DUP1
            // 18     DUP1
            // 19     DUP1
            // 20     CALLDATALOAD
            // 21     PUSH2  => eeee
            // 24     CALL
            // 25     POP
            // 26     PUSH1  => 00
            // 28     DUP1
            // 29     DUP1
            // 30     DUP1
            // 31     PUSH1  => 05
            // 33     DUP2
            // 34     CALLDATALOAD
            // 35     PUSH2  => eeee
            // 38     CALL

            let mut state = InMemoryState::default();
            let mut analysis_cache = AnalysisCache::default();
            let mut engine = engine_factory(MAINNET.clone()).unwrap();
            let block_spec = MAINNET.collect_block_spec(header.number);
            let mut processor = ExecutionProcessor::new(
                &mut state,
                &mut analysis_cache,
                &mut *engine,
                &header,
                &block,
                &block_spec,
            );

            processor
                .state()
                .add_to_balance(originator, *ETHER)
                .await
                .unwrap();
            processor
                .state()
                .set_code(caller_address, caller_code.to_vec().into())
                .await
                .unwrap();
            processor
                .state()
                .set_code(suicidal_address, suicidal_code.to_vec().into())
                .await
                .unwrap();

            let t = |action, input, nonce| TransactionWithSender {
                message: TransactionMessage::EIP1559 {
                    chain_id: MAINNET.params.chain_id,
                    nonce,
                    max_priority_fee_per_gas: U256::from(20 * GIGA),
                    max_fee_per_gas: U256::from(20 * GIGA),
                    gas_limit: 100_000,
                    action,
                    value: U256::zero(),
                    input,
                    access_list: Default::default(),
                },
                sender: originator,
            };

            let txn = (t)(
                TransactionAction::Call(caller_address),
                H256::from(suicidal_address).0.to_vec().into(),
                0,
            );

            let receipt1 = processor.execute_transaction(&txn).await.unwrap();
            assert!(receipt1.success);

            assert!(!processor.state().exists(suicidal_address).await.unwrap());

            // Now the contract is self-destructed, this is a simple value transfer
            let txn = (t)(TransactionAction::Call(suicidal_address), vec![].into(), 1);

            let receipt2 = processor.execute_transaction(&txn).await.unwrap();
            assert!(receipt2.success);

            assert!(processor.state().exists(suicidal_address).await.unwrap());
            assert_eq!(
                processor
                    .state()
                    .get_balance(suicidal_address)
                    .await
                    .unwrap(),
                U256::zero()
            );

            assert_eq!(
                receipt2.cumulative_gas_used,
                receipt1.cumulative_gas_used + fee::G_TRANSACTION,
            );
        })
    }

    #[test]
    fn out_of_gas_during_account_recreation() {
        run_test(async {
            let block_number = 2_081_788.into();
            let header = PartialHeader {
                number: block_number,
                gas_limit: 4_712_388,
                beneficiary: hex!("a42af2c70d316684e57aefcc6e393fecb1c7e84e").into(),
                ..PartialHeader::empty()
            };
            let block = Default::default();
            let caller = hex!("c789e5aba05051b1468ac980e30068e19fad8587").into();

            let nonce = 0;
            let address = create_address(caller, nonce);

            let mut state = InMemoryState::default();

            // Some funds were previously transferred to the address:
            // https://etherscan.io/address/0x78c65b078353a8c4ce58fb4b5acaac6042d591d5
            let account = Account {
                balance: U256::from(66_252_368 * GIGA),
                ..Default::default()
            };
            state
                .update_account(address, None, Some(account.clone()))
                .await
                .unwrap();

            let txn = TransactionWithSender{
                message: TransactionMessage::EIP1559 {
                    chain_id: MAINNET.params.chain_id,
                    nonce,
                    max_priority_fee_per_gas: 0.into(),
                    max_fee_per_gas: U256::from(20 * GIGA),
                    gas_limit: 690_000,
                    action: TransactionAction::Create,
                    value: U256::zero(),
                    access_list: Default::default(),
                    input: hex!(
                        "6060604052604051610ca3380380610ca3833981016040528080518201919060200150505b60028151101561003357610002565b80600060005090805190602001908280548282559060005260206000209081019282156100a4579160200282015b828111156100a35782518260006101000a81548173ffffffffffffffffffffffffffffffffffffffff0219169083021790555091602001919060010190610061565b5b5090506100eb91906100b1565b808211156100e757600081816101000a81549073ffffffffffffffffffffffffffffffffffffffff0219169055506001016100b1565b5090565b50506000600160006101000a81548160ff021916908302179055505b50610b8d806101166000396000f360606040523615610095576000357c0100000000000000000000000000000000000000000000000000000000900480632079fb9a14610120578063391252151461016257806345550a51146102235780637df73e27146102ac578063979f1976146102da578063a0b7967b14610306578063a68a76cc14610329578063abe3219c14610362578063fc0f392d1461038757610095565b61011e5b600034111561011b577f6e89d517057028190560dd200cf6bf792842861353d1173761dfa362e1c133f03334600036604051808573ffffffffffffffffffffffffffffffffffffffff16815260200184815260200180602001828103825284848281815260200192508082843782019150509550505050505060405180910390a15b5b565b005b6101366004808035906020019091905050610396565b604051808273ffffffffffffffffffffffffffffffffffffffff16815260200191505060405180910390f35b6102216004808035906020019091908035906020019091908035906020019082018035906020019191908080601f016020809104026020016040519081016040528093929190818152602001838380828437820191505050505050909091908035906020019091908035906020019091908035906020019082018035906020019191908080601f0160208091040260200160405190810160405280939291908181526020018383808284378201915050505050509090919050506103d8565b005b6102806004808035906020019091908035906020019082018035906020019191908080601f01602080910402602001604051908101604052809392919081815260200183838082843782019150505050505090909190505061064b565b604051808273ffffffffffffffffffffffffffffffffffffffff16815260200191505060405180910390f35b6102c260048080359060200190919050506106fa565b60405180821515815260200191505060405180910390f35b6102f060048080359060200190919050506107a8565b6040518082815260200191505060405180910390f35b6103136004805050610891565b6040518082815260200191505060405180910390f35b6103366004805050610901565b604051808273ffffffffffffffffffffffffffffffffffffffff16815260200191505060405180910390f35b61036f600480505061093b565b60405180821515815260200191505060405180910390f35b610394600480505061094e565b005b600060005081815481101561000257906000526020600020900160005b9150909054906101000a900473ffffffffffffffffffffffffffffffffffffffff1681565b600060006103e5336106fa565b15156103f057610002565b600160009054906101000a900460ff1680156104125750610410886106fa565b155b1561041c57610002565b4285101561042957610002565b610432846107a8565b508787878787604051808673ffffffffffffffffffffffffffffffffffffffff166c010000000000000000000000000281526014018581526020018480519060200190808383829060006004602084601f0104600f02600301f15090500183815260200182815260200195505050505050604051809103902091506104b7828461064b565b90506104c2816106fa565b15156104cd57610002565b3373ffffffffffffffffffffffffffffffffffffffff168173ffffffffffffffffffffffffffffffffffffffff16141561050657610002565b8773ffffffffffffffffffffffffffffffffffffffff16600088604051809050600060405180830381858888f19350505050151561054357610002565b7f59bed9ab5d78073465dd642a9e3e76dfdb7d53bcae9d09df7d0b8f5234d5a8063382848b8b8b604051808773ffffffffffffffffffffffffffffffffffffffff1681526020018673ffffffffffffffffffffffffffffffffffffffff168152602001856000191681526020018473ffffffffffffffffffffffffffffffffffffffff168152602001838152602001806020018281038252838181518152602001915080519060200190808383829060006004602084601f0104600f02600301f150905090810190601f16801561062e5780820380516001836020036101000a031916815260200191505b5097505050505050505060405180910390a15b5050505050505050565b60006000600060006041855114151561066357610002565b602085015192506040850151915060ff6041860151169050601b8160ff16101561069057601b8101905080505b60018682858560405180856000191681526020018460ff16815260200183600019168152602001826000191681526020019450505050506020604051808303816000866161da5a03f1156100025750506040518051906020015093506106f1565b50505092915050565b60006000600090505b600060005080549050811015610799578273ffffffffffffffffffffffffffffffffffffffff16600060005082815481101561000257906000526020600020900160005b9054906101000a900473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16141561078b57600191506107a2565b5b8080600101915050610703565b600091506107a2565b50919050565b6000600060006107b7336106fa565b15156107c257610002565b60009150600090505b600a8160ff16101561084b578360026000508260ff16600a8110156100025790900160005b505414156107fd57610002565b600260005082600a8110156100025790900160005b505460026000508260ff16600a8110156100025790900160005b5054101561083d578060ff16915081505b5b80806001019150506107cb565b600260005082600a8110156100025790900160005b505484101561086e57610002565b83600260005083600a8110156100025790900160005b50819055505b5050919050565b60006000600060009150600090505b600a8110156108f15781600260005082600a8110156100025790900160005b505411156108e357600260005081600a8110156100025790900160005b5054915081505b5b80806001019150506108a0565b6001820192506108fc565b505090565b600061090c336106fa565b151561091757610002565b6040516101c2806109cb833901809050604051809103906000f09050610938565b90565b600160009054906101000a900460ff1681565b610957336106fa565b151561096257610002565b6001600160006101000a81548160ff021916908302179055507f0909e8f76a4fd3e970f2eaef56c0ee6dfaf8b87c5b8d3f56ffce78e825a9115733604051808273ffffffffffffffffffffffffffffffffffffffff16815260200191505060405180910390a15b5660606040525b33600060006101000a81548173ffffffffffffffffffffffffffffffffffffffff021916908302179055505b6101838061003f6000396000f360606040523615610048576000357c0100000000000000000000000000000000000000000000000000000000900480636b9f96ea146100a6578063ca325469146100b557610048565b6100a45b600060009054906101000a900473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16600034604051809050600060405180830381858888f19350505050505b565b005b6100b360048050506100ee565b005b6100c2600480505061015d565b604051808273ffffffffffffffffffffffffffffffffffffffff16815260200191505060405180910390f35b600060009054906101000a900473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff1660003073ffffffffffffffffffffffffffffffffffffffff1631604051809050600060405180830381858888f19350505050505b565b600060009054906101000a900473ffffffffffffffffffffffffffffffffffffffff16815600000000000000000000000000000000000000000000000000000000000000200000000000000000000000000000000000000000000000000000000000000002000000000000000000000000c789e5aba05051b1468ac980e30068e19fad858700000000000000000000000099c426b2a0453e27decaecd93c3722fb0f378fc5"
                    ).to_vec().into(),
                },
                sender: caller,
            };

            let mut analysis_cache = AnalysisCache::default();
            let mut engine = engine_factory(MAINNET.clone()).unwrap();
            let block_spec = MAINNET.collect_block_spec(header.number);
            let mut processor = ExecutionProcessor::new(
                &mut state,
                &mut analysis_cache,
                &mut *engine,
                &header,
                &block,
                &block_spec,
            );
            processor
                .state()
                .add_to_balance(caller, *ETHER)
                .await
                .unwrap();

            let receipt = processor.execute_transaction(&txn).await.unwrap();
            // out of gas
            assert!(!receipt.success);

            processor
                .into_state()
                .write_to_db(block_number)
                .await
                .unwrap();

            // only the caller and the miner should change
            assert_eq!(state.read_account(address).await.unwrap(), Some(account));
        })
    }

    #[test]
    fn empty_suicide_beneficiary() {
        run_test(async {
            let block_number = 2_687_389.into();
            let header = PartialHeader {
                number: block_number,
                gas_limit: 4_712_388,
                beneficiary: hex!("2a65aca4d5fc5b5c859090a6c34d164135398226").into(),
                ..PartialHeader::empty()
            };
            let block = Default::default();
            let caller = hex!("5ed8cee6b63b1c6afce3ad7c92f4fd7e1b8fad9f").into();
            let suicide_beneficiary = hex!("ee098e6c2a43d9e2c04f08f0c3a87b0ba59079d5").into();

            let txn = TransactionWithSender {
                message: TransactionMessage::EIP1559 {
                    chain_id: MAINNET.params.chain_id,
                    nonce: 0,
                    max_priority_fee_per_gas: U256::zero(),
                    max_fee_per_gas: U256::from(30 * GIGA),
                    gas_limit: 360_000,
                    action: TransactionAction::Create,
                    value: U256::zero(),
                    input: hex!(
                        "6000607f5359610043806100135939610056566c010000000000000000000000007fee098e6c2a43d9e2c04f08f0c3a87b0ba59079d4d53532071d6cd0cb86facd5605ff6100008061003f60003961003f565b6000f35b816000f0905050596100718061006c59396100dd5661005f8061000e60003961006d566000603f5359610043806100135939610056566c010000000000000000000000007fee098e6c2a43d9e2c04f08f0c3a87b0ba59079d4d53532071d6cd0cb86facd5605ff6100008061003f60003961003f565b6000f35b816000f0905050fe5b6000f35b816000f0905060405260006000600060006000604051620249f0f15061000080610108600039610108565b6000f3"
                    ).to_vec().into(),
                    access_list: Default::default(),
                },
                sender: caller,
            };

            let mut state = InMemoryState::default();
            let mut analysis_cache = AnalysisCache::default();
            let mut engine = engine_factory(MAINNET.clone()).unwrap();
            let block_spec = MAINNET.collect_block_spec(header.number);
            let mut processor = ExecutionProcessor::new(
                &mut state,
                &mut analysis_cache,
                &mut *engine,
                &header,
                &block,
                &block_spec,
            );

            processor
                .state()
                .add_to_balance(caller, *ETHER)
                .await
                .unwrap();

            let receipt = processor.execute_transaction(&txn).await.unwrap();
            assert!(receipt.success);

            processor
                .into_state()
                .write_to_db(block_number)
                .await
                .unwrap();

            // suicide_beneficiary should've been touched and deleted
            assert_eq!(state.read_account(suicide_beneficiary).await.unwrap(), None);
        })
    }
}
