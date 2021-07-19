use self::processor::ExecutionProcessor;
use crate::{crypto::ordered_trie_root, models::*, State};
use ethereum_types::H256;
use rlp::Encodable;

mod address;
pub mod evm;
pub mod precompiled;
pub mod processor;

pub async fn execute_block<'storage, S: State<'storage>>(
    state: &mut S,
    config: &ChainConfig,
    header: &BlockHeader,
    block: &BlockBodyWithSenders,
) -> anyhow::Result<Vec<Receipt>> {
    Ok(ExecutionProcessor::new(state, header, block, config)
        .execute_and_write_block()
        .await?)
}

pub fn root_hash<E: Encodable>(values: &[E]) -> H256 {
    ordered_trie_root(values.iter().map(rlp::encode))
}

#[cfg(test)]
mod tests {
    use super::{address::create_address, *};
    use crate::{
        chain::{config::MAINNET_CONFIG, protocol_param::param},
        common::*,
        util::test_util::run_test,
        InMemoryState, DEFAULT_INCARNATION,
    };
    use ethereum_types::*;
    use hex_literal::hex;

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
        run_test(async {
            // ---------------------------------------
            // Prepare
            // ---------------------------------------

            let block_number = 13_500_001;
            let miner = hex!("5a0b54d5dc17e0aadc383d2db43b0a0d3e029c4c").into();

            let gas_used = 98_824;
            let mut receipts = vec![Receipt {
                tx_type: TxType::EIP1559,
                success: true,
                cumulative_gas_used: gas_used,
                bloom: Bloom::zero(),
                logs: vec![],
            }];

            let header = BlockHeader {
                number: block_number,
                beneficiary: miner,
                gas_limit: 100_000,
                gas_used,

                parent_hash: Default::default(),
                ommers_hash: Default::default(),
                state_root: Default::default(),
                transactions_root: Default::default(),
                receipts_root: root_hash(&receipts),
                logs_bloom: Default::default(),
                difficulty: Default::default(),
                timestamp: Default::default(),
                extra_data: Default::default(),
                mix_hash: Default::default(),
                nonce: Default::default(),
                base_fee_per_gas: Default::default(),
            };

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

            let tx = TransactionWithSender {
                tx_type: TxType::EIP1559,
                input: deployment_code.into(),
                gas_limit: header.gas_limit,
                max_priority_fee_per_gas: U256::zero(), // EIP-1559
                max_fee_per_gas: U256::from(20 * GIGA),
                sender,
                action: TransactionAction::Create,

                chain_id: None,
                nonce: 0,
                value: U256::zero(),
                access_list: Default::default(),
            };

            let mut state = InMemoryState::default();
            let sender_account = Account {
                balance: ETHER.into(),
                ..Default::default()
            };
            state
                .update_account(sender, None, Some(sender_account))
                .await
                .unwrap();

            // ---------------------------------------
            // Execute first block
            // ---------------------------------------

            execute_block(
                &mut state,
                &MAINNET_CONFIG,
                &header,
                &BlockBodyWithSenders {
                    transactions: vec![tx.clone()],
                    ommers: vec![],
                },
            )
            .await
            .unwrap();

            let contract_address = create_address(sender, 0);
            let contract_account = state.read_account(contract_address).await.unwrap().unwrap();

            let code_hash = hash_data(contract_code);
            assert_eq!(contract_account.code_hash, code_hash);

            let storage_key0 = H256::zero();
            let storage0 = state
                .read_storage(contract_address, DEFAULT_INCARNATION, storage_key0)
                .await
                .unwrap();
            assert_eq!(
                storage0,
                hex!("000000000000000000000000000000000000000000000000000000000000002a").into()
            );

            let storage_key1 =
                hex!("0000000000000000000000000000000000000000000000000000000000000001").into();
            let storage1 = state
                .read_storage(contract_address, DEFAULT_INCARNATION, storage_key1)
                .await
                .unwrap();
            assert_eq!(
                storage1,
                hex!("00000000000000000000000000000000000000000000000000000000000001c9").into()
            );

            let miner_account = state.read_account(miner).await.unwrap().unwrap();
            assert_eq!(
                miner_account.balance,
                param::BLOCK_REWARD_CONSTANTINOPLE.into()
            );

            // ---------------------------------------
            // Execute second block
            // ---------------------------------------

            let new_val = hex!("000000000000000000000000000000000000000000000000000000000000003e");

            let block_number = 13_500_002;
            let mut header = header.clone();

            header.number = block_number;

            let gas_used = 26_149;
            header.gas_used = gas_used;
            receipts[0].cumulative_gas_used = gas_used;
            header.receipts_root = root_hash(&receipts);

            let mut tx = tx.clone();

            tx.nonce = 1;
            tx.action = TransactionAction::Call(contract_address);
            tx.input = new_val.to_vec().into();
            tx.max_priority_fee_per_gas = U256::from(20 * GIGA);

            execute_block(
                &mut state,
                &MAINNET_CONFIG,
                &header,
                &BlockBodyWithSenders {
                    transactions: vec![tx],
                    ommers: vec![],
                },
            )
            .await
            .unwrap();

            let storage0 = state
                .read_storage(contract_address, DEFAULT_INCARNATION, storage_key0)
                .await
                .unwrap();
            assert_eq!(storage0, new_val.into());

            let miner_account = state.read_account(miner).await.unwrap().unwrap();
            assert!(miner_account.balance > U256::from(2 * param::BLOCK_REWARD_CONSTANTINOPLE));
            assert!(miner_account.balance < U256::from(3 * param::BLOCK_REWARD_CONSTANTINOPLE));
        })
    }
}
