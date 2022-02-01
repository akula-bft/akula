use crate::{crypto::*, models::*, util::*, State};
use async_trait::async_trait;
use bytes::Bytes;
use std::{collections::HashMap, convert::TryInto};

// address -> initial value
type AccountChanges = HashMap<Address, Option<Account>>;

// address -> location -> initial value
type StorageChanges = HashMap<Address, HashMap<U256, U256>>;

/// Holds all state in memory.
#[derive(Debug, Default)]
pub struct InMemoryState {
    accounts: HashMap<Address, Account>,

    // hash -> code
    code: HashMap<H256, Bytes>,

    // address -> location -> value
    storage: HashMap<Address, HashMap<U256, U256>>,

    // block number -> hash -> header
    headers: Vec<HashMap<H256, BlockHeader>>,

    // block number -> hash -> body
    bodies: Vec<HashMap<H256, BlockBody>>,

    // block number -> hash -> total difficulty
    difficulty: Vec<HashMap<H256, U256>>,

    canonical_hashes: Vec<H256>,
    // per block
    account_changes: HashMap<BlockNumber, AccountChanges>,
    // per block
    storage_changes: HashMap<BlockNumber, StorageChanges>,

    block_number: BlockNumber,
}

impl InMemoryState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn accounts(&self) -> impl Iterator<Item = (Address, &Account)> {
        self.accounts.iter().map(|(&k, v)| (k, v))
    }

    pub fn account_addresses(&self) -> impl Iterator<Item = Address> + '_ {
        self.accounts.keys().copied()
    }

    // https://eth.wiki/fundamentals/patricia-tree#storage-trie
    fn account_storage_root(&self, address: Address) -> H256 {
        if let Some(storage) = self.storage.get(&address) {
            if !storage.is_empty() {
                return trie_root(storage.iter().map(|(&location, &value)| {
                    let value = u256_to_h256(value);
                    let zv = zeroless_view(&value);
                    let encoded_location = keccak256(u256_to_h256(location));
                    let encoded_value = rlp::encode(&zv);
                    (encoded_location, encoded_value)
                }));
            }
        }

        EMPTY_ROOT
    }

    pub fn number_of_accounts(&self) -> u64 {
        self.accounts.len().try_into().unwrap()
    }

    pub fn storage_size(&self, address: Address) -> u64 {
        if let Some(storage) = self.storage.get(&address) {
            return storage.len().try_into().unwrap();
        }

        0
    }

    pub fn state_root_hash(&self) -> H256 {
        if self.accounts.is_empty() {
            return EMPTY_ROOT;
        }

        trie_root(self.accounts.iter().map(|(&address, account)| {
            let storage_root = self.account_storage_root(address);
            let account = account.to_rlp(storage_root);
            (keccak256(address), rlp::encode(&account))
        }))
    }

    pub fn current_canonical_block(&self) -> BlockNumber {
        BlockNumber(self.canonical_hashes.len() as u64 - 1)
    }

    pub fn canonical_hash(&self, block_number: BlockNumber) -> Option<H256> {
        self.canonical_hashes.get(block_number.0 as usize).copied()
    }

    pub fn insert_block(&mut self, block: Block, hash: H256) {
        let Block {
            header,
            transactions,
            ommers,
        } = block;

        let block_number = header.number.0 as usize;
        let parent_hash = header.parent_hash;
        let difficulty = header.difficulty;

        if self.headers.len() <= block_number {
            self.headers.resize_with(block_number + 1, Default::default);
        }
        self.headers[block_number].insert(hash, header);

        if self.bodies.len() <= block_number {
            self.bodies.resize_with(block_number + 1, Default::default);
        }
        self.bodies[block_number].insert(
            hash,
            BlockBody {
                transactions,
                ommers,
            },
        );

        if self.difficulty.len() <= block_number {
            self.difficulty
                .resize_with(block_number + 1, Default::default);
        }

        let d = {
            if block_number == 0 {
                U256::ZERO
            } else {
                *self.difficulty[block_number - 1]
                    .entry(parent_hash)
                    .or_default()
            }
        } + difficulty;
        self.difficulty[block_number].entry(hash).insert(d);
    }

    pub fn read_body_with_senders(
        &self,
        block_number: BlockNumber,
        block_hash: H256,
    ) -> anyhow::Result<Option<BlockBodyWithSenders>> {
        if let Some(body_map) = self.bodies.get(block_number.0 as usize) {
            return body_map
                .get(&block_hash)
                .map(|body| {
                    Ok(BlockBodyWithSenders {
                        transactions: body
                            .transactions
                            .iter()
                            .map(|tx| {
                                let sender = tx.recover_sender()?;
                                Ok(MessageWithSender {
                                    message: tx.message.clone(),
                                    sender,
                                })
                            })
                            .collect::<anyhow::Result<_>>()?,
                        ommers: body.ommers.clone(),
                    })
                })
                .transpose();
        }

        Ok(None)
    }

    pub fn canonize_block(&mut self, block_number: BlockNumber, block_hash: H256) {
        let block_number = block_number.0 as usize;

        if self.canonical_hashes.len() <= block_number {
            self.canonical_hashes
                .resize_with(block_number + 1, Default::default);
        }

        self.canonical_hashes[block_number] = block_hash;
    }

    pub fn decanonize_block(&mut self, block_number: BlockNumber) {
        self.canonical_hashes.truncate(block_number.0 as usize);
    }

    pub fn unwind_state_changes(&mut self, block_number: BlockNumber) {
        for (address, account) in self
            .account_changes
            .remove(&block_number)
            .unwrap_or_default()
        {
            if let Some(account) = account {
                self.accounts.insert(address, account);
            } else {
                self.accounts.remove(&address);
            }
        }

        for (address, storage) in self
            .storage_changes
            .remove(&block_number)
            .unwrap_or_default()
        {
            for (location, value) in storage {
                let e = self.storage.entry(address).or_default();
                if value == 0 {
                    e.remove(&location);
                } else {
                    e.insert(location, value);
                }
            }
        }
    }
}

#[async_trait]
impl State for InMemoryState {
    // Readers

    async fn read_account(&self, address: Address) -> anyhow::Result<Option<Account>> {
        Ok(self.accounts.get(&address).cloned())
    }

    async fn read_code(&self, code_hash: H256) -> anyhow::Result<Bytes> {
        Ok(self.code.get(&code_hash).cloned().unwrap_or_default())
    }

    async fn read_storage(&self, address: Address, location: U256) -> anyhow::Result<U256> {
        if let Some(storage) = self.storage.get(&address) {
            if let Some(value) = storage.get(&location) {
                return Ok(*value);
            }
        }

        Ok(U256::ZERO)
    }

    async fn erase_storage(&mut self, address: Address) -> anyhow::Result<()> {
        let address_storage = self.storage.remove(&address).unwrap_or_default();

        if !address_storage.is_empty() {
            let storage_changes = self
                .storage_changes
                .entry(self.block_number)
                .or_default()
                .entry(address)
                .or_default();

            for (slot, initial) in address_storage {
                storage_changes.insert(slot, initial);
            }
        }

        Ok(())
    }

    async fn read_header(
        &self,
        block_number: BlockNumber,
        block_hash: H256,
    ) -> anyhow::Result<Option<BlockHeader>> {
        if let Some(header_map) = self.headers.get(block_number.0 as usize) {
            return Ok(header_map.get(&block_hash).cloned());
        }

        Ok(None)
    }

    async fn read_body(
        &self,
        block_number: BlockNumber,
        block_hash: H256,
    ) -> anyhow::Result<Option<BlockBody>> {
        if let Some(body_map) = self.bodies.get(block_number.0 as usize) {
            return Ok(body_map.get(&block_hash).cloned());
        }

        Ok(None)
    }

    async fn total_difficulty(
        &self,
        block_number: BlockNumber,
        block_hash: H256,
    ) -> anyhow::Result<Option<U256>> {
        if let Some(difficulty_map) = self.difficulty.get(block_number.0 as usize) {
            return Ok(difficulty_map.get(&block_hash).cloned());
        }

        Ok(None)
    }

    /// State changes
    /// Change sets are backward changes of the state, i.e. account/storage values _at the beginning of a block_.

    /// Mark the beggining of a new block.
    /// Must be called prior to calling update_account/update_account_code/update_storage.
    fn begin_block(&mut self, block_number: BlockNumber) {
        self.block_number = block_number;
    }

    fn update_account(
        &mut self,
        address: Address,
        initial: Option<Account>,
        current: Option<Account>,
    ) {
        self.account_changes
            .entry(self.block_number)
            .or_default()
            .insert(address, initial);

        if let Some(current) = current {
            self.accounts.insert(address, current);
        } else {
            self.accounts.remove(&address);
        }
    }

    async fn update_code(&mut self, code_hash: H256, code: Bytes) -> anyhow::Result<()> {
        self.code.insert(code_hash, code);

        Ok(())
    }

    async fn update_storage(
        &mut self,
        address: Address,
        location: U256,
        initial: U256,
        current: U256,
    ) -> anyhow::Result<()> {
        self.storage_changes
            .entry(self.block_number)
            .or_default()
            .entry(address)
            .or_default()
            .insert(location, initial);

        let e = self.storage.entry(address).or_default();

        if current == 0 {
            e.remove(&location);
        } else {
            e.insert(location, current);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::*;
    use hex_literal::hex;
    use maplit::*;

    #[test]
    fn state_root() {
        run_test(async {
            fn to_u256((a, b): (u128, u128)) -> (U256, U256) {
                (U256::from(a), U256::from(b))
            }

            let fixtures = vec![
                (
                    "gasLimit20m_London",
                    vec![
                        (
                            hex!("2adc25665018aa1fe0e6bc666dac8fc2697ff9ba"),
                            SerializedAccount {
                                code: hex!("").to_vec().into(),
                                nonce: 0x00_u64.into(),
                                balance: 0x8ac7230489e80000_u128.into(),
                                storage: hashmap! {},
                            },
                        ),
                        (
                            hex!("a94f5374fce5edbc8e2a8697c15331677e6ebf0b"),
                            SerializedAccount {
                                code: hex!("").to_vec().into(),
                                nonce: 0x01_u64.into(),
                                balance: 0x010000000000_u128.into(),
                                storage: hashmap! {},
                            },
                        ),
                        (
                            hex!("d02d72e067e77158444ef2020ff2d325f929b363"),
                            SerializedAccount {
                                code: hex!("").to_vec().into(),
                                nonce: 0x01_u64.into(),
                                balance: 0x01000000000000_u128.into(),
                                storage: hashmap! {},
                            },
                        ),
                    ],
                    hex!("fa8821769befe30fdbb2021514d56dca740f3422bd2f8d2742f4972e400de910"),
                ),
                (
                    "baseFee_London",
                    vec![
                        (
                            hex!("000000000000000000000000000000000000c0de"),
                            SerializedAccount {
                                code: hex!("600060006000600073cccccccccccccccccccccccccccccccccccccccc5af450")
                                .to_vec()
                                .into(),
                                nonce: 0x01_u64.into(),
                                balance: 0x3000_u64.into(),
                                storage: vec![
                                    (0x02, 0x02fe),
                                    (0x03, 0x029f),
                                    (0x04, 0x024c),
                                    (0x1002, 0x64),
                                    (0x1003, 0x64),
                                    (0x1004, 0x64),
                                    (0x2002, 0x1000),
                                    (0x2003, 0x2000),
                                    (0x2004, 0x3000),
                                ]
                                .into_iter()
                                .map(to_u256)
                                .collect(),
                            },
                        ),
                        (
                            hex!("a94f5374fce5edbc8e2a8697c15331677e6ebf0b"),
                            SerializedAccount {
                                code: vec![].into(),
                                nonce: 0x00_u64.into(),
                                balance: 0x01000000000000000000_u128.into(),
                                storage: hashmap! {},
                            },
                        ),
                        (
                            hex!("ba5e000000000000000000000000000000000000"),
                            SerializedAccount {
                                code: vec![].into(),
                                nonce: 0x00_u64.into(),
                                balance: 0x6f05b5a16c783b4b_u128.into(),
                                storage: hashmap! {},
                            },
                        ),
                        (
                            hex!("cccccccccccccccccccccccccccccccccccccccc"),
                            SerializedAccount {
                                code: hex!("484355483a036110004301554761200043015500")
                                    .to_vec()
                                    .into(),
                                nonce: 0x01_u64.into(),
                                balance: 0x010000000000_u128.into(),
                                storage: vec![
                                    (0x01, 0x036b),
                                    (0x02, 0x02fe),
                                    (0x03, 0x029f),
                                    (0x04, 0x024c),
                                    (0x1001, 0x01),
                                    (0x1002, 0x0a),
                                    (0x1003, 0x0149),
                                    (0x1004, 0x019c),
                                    (0x2001, 0x010000000000),
                                    (0x2002, 0x010000000000),
                                    (0x2003, 0x010000000000),
                                    (0x2004, 0x010000000000),
                                ]
                                .into_iter()
                                .map(to_u256)
                                .collect(),
                            },
                        ),
                        (hex!("cccccccccccccccccccccccccccccccccccccccd"), SerializedAccount {
                            code : hex!("600060006000600073cccccccccccccccccccccccccccccccccccccccc5af450").to_vec().into(),
                            nonce : 0x01_u64.into(),
                            balance : 0x020000000000_u128.into(),
                            storage : vec![
                                (0x02, 0x02fe),
                                (0x03, 0x029f),
                                (0x04, 0x024c),
                                (0x1002, 0x64),
                                (0x1003, 0x018401),
                                (0x1004, 0x018454),
                                (0x2002, 0x020000000000),
                                (0x2003, 0x020000000000),
                                (0x2004, 0x020000000000)
                            ].into_iter().map(to_u256).collect(),
                        }),
                        (hex!("ccccccccccccccccccccccccccccccccccccccce"), SerializedAccount {
                            code : hex!("600060006000600061100061c0de5af1600060006000600073cccccccccccccccccccccccccccccccccccccccc5af4905050").to_vec().into(),
                            nonce : 0x01_u64.into(),
                            balance : 0x01ffffffd000_u128.into(),
                            storage : vec![
                                (0x02, 0x02fe),
                                (0x03, 0x029f),
                                (0x04, 0x024c),
                                (0x1002, 0x64),
                                (0x1003, 0x64),
                                (0x1004, 0x64),
                                (0x2002, 0x01fffffff000),
                                (0x2003, 0x01ffffffe000),
                                (0x2004, 0x01ffffffd000)
                            ].into_iter().map(to_u256).collect(),
                        }),
                        (
                            hex!("d02d72e067e77158444ef2020ff2d325f929b363"),
                            SerializedAccount {
                                code: vec![].into(),
                                nonce: 0x0b_u64.into(),
                                balance: 0xfffffffffba0afe5e7_u128.into(),
                                storage: hashmap! {},
                            },
                        )
                    ],
                    hex!("2175eed13f198e774ed43abd7f5c912eae7c15ebd7422ff09de138f97ab77328")
                ),
            ];

            for (test_name, fixture, state_root) in fixtures {
                let mut state = InMemoryState::default();

                println!("{}", test_name);
                for (address, account) in fixture {
                    let address = Address::from(address);
                    state.update_account(
                        address,
                        None,
                        Some(Account {
                            nonce: account.nonce.as_u64(),
                            balance: account.balance,
                            code_hash: keccak256(account.code),
                        }),
                    );

                    for (location, value) in account.storage {
                        state
                            .update_storage(address, location, U256::ZERO, value)
                            .await
                            .unwrap();
                    }
                }

                assert_eq!(state.state_root_hash(), H256(state_root))
            }
        })
    }
}
