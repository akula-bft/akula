use self::{analysis_cache::AnalysisCache, processor::ExecutionProcessor, tracer::NoopTracer};
use crate::{
    consensus::{self, DuoError},
    models::*,
    State,
};

pub mod address;
pub mod analysis_cache;
pub mod evm;
pub mod evmglue;
pub mod precompiled;
pub mod processor;
pub mod tracer;

pub fn execute_block<S: State>(
    state: &mut S,
    config: &ChainSpec,
    header: &BlockHeader,
    block: &BlockBodyWithSenders,
) -> Result<Vec<Receipt>, DuoError> {
    let mut analysis_cache = AnalysisCache::default();
    let mut engine = consensus::engine_factory(None, config.clone(), None)?;
    let mut tracer = NoopTracer;

    let block_config = config.collect_block_spec(header.number);
    ExecutionProcessor::new(
        state,
        &mut tracer,
        &mut analysis_cache,
        &mut *engine,
        header,
        block,
        &block_config,
        config,
    )
    .execute_and_write_block()
}

#[cfg(test)]
mod tests {
    use super::{address::create_address, *};
    use crate::{
        res::chainspec::MAINNET, trie::root_hash, InMemoryState, StateReader, StateWriter,
    };
    use hex_literal::hex;
    use sha3::{Digest, Keccak256};

    #[test]
    fn compute_receipt_root() {
        let receipts = vec![
            Receipt::new(TxType::Legacy, true, 21_000, vec![]),
            Receipt::new(TxType::Legacy, true, 42_000, vec![]),
            Receipt::new(
                TxType::Legacy,
                true,
                65_092,
                vec![Log {
                    address: hex!("8d12a197cb00d4747a1fe03395095ce2a5cc6819").into(),
                    topics: vec![hex!(
                        "f341246adaac6f497bc2a656f546ab9e182111d630394f0c57c710a59a2cb567"
                    )
                    .into()],
                    data: hex!("000000000000000000000000000000000000000000000000000000000000000000000000000000000000000043b2126e7a22e0c288dfb469e3de4d2c097f3ca0000000000000000000000000000000000000000000000001195387bce41fd4990000000000000000000000000000000000000000000000000000000000000000").to_vec().into(),
                }],
            ),
        ];

        assert_eq!(
            root_hash(&receipts),
            hex!("7ea023138ee7d80db04eeec9cf436dc35806b00cc5fe8e5f611fb7cf1b35b177").into()
        )
    }

    #[test]
    fn execute_two_blocks() {
        const BLOCK_REWARD_CONSTANTINOPLE: u128 = 2 * ETHER;

        // ---------------------------------------
        // Prepare
        // ---------------------------------------

        let block_number = 13_500_001.into();
        let miner = hex!("5a0b54d5dc17e0aadc383d2db43b0a0d3e029c4c").into();

        let gas_used = 98_824;
        let mut receipts = vec![Receipt {
            tx_type: TxType::EIP1559,
            success: true,
            cumulative_gas_used: gas_used,
            bloom: Bloom::zero(),
            logs: vec![],
        }];

        let header = PartialHeader {
            number: block_number,
            beneficiary: miner,
            gas_limit: 100_000,
            gas_used,
            receipts_root: root_hash(&receipts),
            ..PartialHeader::empty()
        };
        let header = BlockHeader::new(header, EMPTY_LIST_HASH, EMPTY_ROOT);

        // This contract initially sets its 0th storage to 0x2a
        // and its 1st storage to 0x01c9.
        // When called, it updates its 0th storage to the input provided.
        let contract_code = hex!("600035600055");
        let deployment_code = std::iter::empty()
            .chain(&hex!("602a6000556101c960015560068060166000396000f3") as &[u8])
            .chain(&contract_code)
            .copied()
            .collect::<Vec<u8>>();

        let sender = hex!("b685342b8c54347aad148e1f22eff3eb3eb29391").into();

        let t = |action, input, nonce, max_priority_fee_per_gas: u128| MessageWithSender {
            message: Message::EIP1559 {
                input,
                max_priority_fee_per_gas: max_priority_fee_per_gas.as_u256(),
                action,
                nonce,

                gas_limit: header.gas_limit,
                max_fee_per_gas: U256::from(20 * GIGA),
                chain_id: ChainId(1),

                value: U256::ZERO,
                access_list: Default::default(),
            },
            sender,
        };

        let tx = (t)(TransactionAction::Create, deployment_code.into(), 0, 0);

        let mut state = InMemoryState::default();
        let sender_account = Account {
            balance: ETHER.into(),
            ..Default::default()
        };
        state.update_account(sender, None, Some(sender_account));

        // ---------------------------------------
        // Execute first block
        // ---------------------------------------

        execute_block(
            &mut state,
            &MAINNET,
            &header,
            &BlockBodyWithSenders {
                transactions: vec![tx],
                ommers: vec![],
            },
        )
        .unwrap();

        let contract_address = create_address(sender, 0);
        let contract_account = state.read_account(contract_address).unwrap().unwrap();

        let code_hash = H256::from_slice(&Keccak256::digest(&contract_code)[..]);
        assert_eq!(contract_account.code_hash, code_hash);

        let storage_key0 = U256::ZERO;
        let storage0 = state.read_storage(contract_address, storage_key0).unwrap();
        assert_eq!(storage0, 0x2a);

        let storage_key1 = 0x01.as_u256();
        let storage1 = state.read_storage(contract_address, storage_key1).unwrap();
        assert_eq!(storage1, 0x01c9);

        let miner_account = state.read_account(miner).unwrap().unwrap();
        assert_eq!(miner_account.balance, BLOCK_REWARD_CONSTANTINOPLE);

        // ---------------------------------------
        // Execute second block
        // ---------------------------------------

        let new_val = 0x3e;

        let block_number = 13_500_002.into();
        let mut header = header.clone();

        header.number = block_number;

        let gas_used = 26_149;
        header.gas_used = gas_used;
        receipts[0].cumulative_gas_used = gas_used;
        header.receipts_root = root_hash(&receipts);

        let tx = (t)(
            TransactionAction::Call(contract_address),
            new_val.as_u256().to_be_bytes().to_vec().into(),
            1,
            20_u128 * u128::from(GIGA),
        );

        execute_block(
            &mut state,
            &MAINNET,
            &header,
            &BlockBodyWithSenders {
                transactions: vec![tx],
                ommers: vec![],
            },
        )
        .unwrap();

        let storage0 = state.read_storage(contract_address, storage_key0).unwrap();
        assert_eq!(storage0, new_val);

        let miner_account = state.read_account(miner).unwrap().unwrap();
        assert!(miner_account.balance > 2 * BLOCK_REWARD_CONSTANTINOPLE);
        assert!(miner_account.balance < 3 * BLOCK_REWARD_CONSTANTINOPLE);
    }
}
