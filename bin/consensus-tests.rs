use akula::{
    chain::{
        blockchain::Blockchain,
        config::*,
        consensus::{Consensus, NoProof},
        difficulty::canonical_difficulty,
        validity::pre_validate_transaction,
    },
    crypto::keccak256,
    models::*,
    *,
};
use anyhow::*;
use bytes::Bytes;
use educe::Educe;
use ethereum_types::*;
use maplit::hashmap;
use once_cell::sync::Lazy;
use serde::{de, Deserialize};
use serde_json::{Map, Value};
use std::{
    collections::{HashMap, HashSet},
    convert::TryInto,
    fmt::Debug,
    future::Future,
    ops::AddAssign,
    path::{Path, PathBuf},
    str::FromStr,
};
use structopt::StructOpt;
use tracing::*;
use tracing_subscriber::{prelude::*, EnvFilter};

pub static DIFFICULTY_DIR: Lazy<PathBuf> = Lazy::new(|| Path::new("BasicTests").to_path_buf());
pub static BLOCKCHAIN_DIR: Lazy<PathBuf> = Lazy::new(|| Path::new("BlockchainTests").to_path_buf());
pub static TRANSACTION_DIR: Lazy<PathBuf> =
    Lazy::new(|| Path::new("TransactionTests").to_path_buf());

pub static EXCLUDED_TESTS: Lazy<Vec<PathBuf>> = Lazy::new(|| {
    vec![
        // Very slow tests
        BLOCKCHAIN_DIR
            .join("GeneralStateTests")
            .join("stTimeConsuming"),
        // We do not have extra data check
        BLOCKCHAIN_DIR
            .join("TransitionTests")
            .join("bcHomesteadToDao"),
        // Nonce >= 2^64 is not supported.
        // Geth excludes this test as well:
        // https://github.com/ethereum/go-ethereum/blob/v1.9.25/tests/transaction_test.go#L40
        TRANSACTION_DIR
            .join("ttNonce")
            .join("TransactionWithHighNonce256.json"),
        // Gas limit >= 2^64 is not supported; see EIP-1985.
        // Geth excludes this test as well:
        // https://github.com/ethereum/go-ethereum/blob/v1.9.25/tests/transaction_test.go#L31
        TRANSACTION_DIR
            .join("ttGasLimit")
            .join("TransactionWithGasLimitxPriceOverflow.json"),
        TRANSACTION_DIR
            .join("ttGasLimit")
            .join("TransactionWithHighGas.json"),
        // No chain has such high ID in practice
        TRANSACTION_DIR
            .join("ttVValue")
            .join("V_overflow64bitSigned.json"),
        // These are not valid post-Frontier anyway
        TRANSACTION_DIR
            .join("ttRSValue")
            .join("TransactionWithSvalueHigh.json"),
        TRANSACTION_DIR
            .join("ttRSValue")
            .join("TransactionWithSvalueLargerThan_c_secp256k1n_x05.json"),
    ]
});

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Deserialize)]
enum Network {
    Frontier,
    Homestead,
    EIP150,
    EIP158,
    Byzantium,
    Constantinople,
    ConstantinopleFix,
    Istanbul,
    Berlin,
    London,
    FrontierToHomesteadAt5,
    HomesteadToEIP150At5,
    HomesteadToDaoAt5,
    EIP158ToByzantiumAt5,
    ByzantiumToConstantinopleFixAt5,
    BerlinToLondonAt5,
    EIP2384,
}

impl FromStr for Network {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "Frontier" => Self::Frontier,
            "Homestead" => Self::Homestead,
            "EIP150" => Self::EIP150,
            "EIP158" => Self::EIP158,
            "Byzantium" => Self::Byzantium,
            "Constantinople" => Self::Constantinople,
            "ConstantinopleFix" => Self::ConstantinopleFix,
            "Istanbul" => Self::Istanbul,
            "Berlin" => Self::Berlin,
            "London" => Self::London,
            "FrontierToHomesteadAt5" => Self::FrontierToHomesteadAt5,
            "HomesteadToEIP150At5" => Self::HomesteadToEIP150At5,
            "HomesteadToDaoAt5" => Self::HomesteadToDaoAt5,
            "EIP158ToByzantiumAt5" => Self::EIP158ToByzantiumAt5,
            "ByzantiumToConstantinopleFixAt5" => Self::ByzantiumToConstantinopleFixAt5,
            "BerlinToLondonAt5" => Self::BerlinToLondonAt5,
            "EIP2384" => Self::EIP2384,
            _ => return Err(()),
        })
    }
}

static NETWORK_CONFIG: Lazy<HashMap<Network, ChainConfig>> = Lazy::new(|| {
    hashmap! {
        Network::Frontier => ChainConfig {
            chain_id: 1,
            ..ChainConfig::default()
        },
        Network::Homestead => ChainConfig {
            chain_id: 1,
            homestead_block: Some(0),
            ..ChainConfig::default()
        },
        Network::EIP150 => ChainConfig {
            chain_id: 1,
            homestead_block: Some(0),
            tangerine_block: Some(0),
            ..ChainConfig::default()
        },
        Network::EIP158 => ChainConfig {
            chain_id: 1,
            homestead_block: Some(0),
            tangerine_block: Some(0),
            spurious_block: Some(0),
            ..ChainConfig::default()
        },
        Network::Byzantium => ChainConfig {
            chain_id: 1,
            homestead_block: Some(0),
            tangerine_block: Some(0),
            spurious_block: Some(0),
            byzantium_block: Some(0),
            ..ChainConfig::default()
        },
        Network::Constantinople => ChainConfig {
            chain_id: 1,
            homestead_block: Some(0),
            tangerine_block: Some(0),
            spurious_block: Some(0),
            byzantium_block: Some(0),
            constantinople_block: Some(0),
            ..ChainConfig::default()
        },
        Network::ConstantinopleFix => ChainConfig {
            chain_id: 1,
            homestead_block: Some(0),
            tangerine_block: Some(0),
            spurious_block: Some(0),
            byzantium_block: Some(0),
            constantinople_block: Some(0),
            petersburg_block: Some(0),
            ..ChainConfig::default()
        },
        Network::Istanbul => ChainConfig {
            chain_id: 1,
            homestead_block: Some(0),
            tangerine_block: Some(0),
            spurious_block: Some(0),
            byzantium_block: Some(0),
            constantinople_block: Some(0),
            petersburg_block: Some(0),
            istanbul_block: Some(0),
            ..ChainConfig::default()
        },
        Network::Berlin => ChainConfig {
            chain_id: 1,
            homestead_block: Some(0),
            tangerine_block: Some(0),
            spurious_block: Some(0),
            byzantium_block: Some(0),
            constantinople_block: Some(0),
            petersburg_block: Some(0),
            istanbul_block: Some(0),
            muir_glacier_block: Some(0),
            berlin_block: Some(0),
            ..ChainConfig::default()
        },
        Network::London => ChainConfig {
            chain_id: 1,
            homestead_block: Some(0),
            tangerine_block: Some(0),
            spurious_block: Some(0),
            byzantium_block: Some(0),
            constantinople_block: Some(0),
            petersburg_block: Some(0),
            istanbul_block: Some(0),
            muir_glacier_block: Some(0),
            berlin_block: Some(0),
            london_block: Some(0),
            ..ChainConfig::default()
        },
        Network::FrontierToHomesteadAt5 => ChainConfig {
            chain_id: 1,
            homestead_block: Some(5),
            ..ChainConfig::default()
        },
        Network::HomesteadToEIP150At5 => ChainConfig {
            chain_id: 1,
            homestead_block: Some(0),
            tangerine_block: Some(5),
            ..ChainConfig::default()
        },
        Network::HomesteadToDaoAt5 => ChainConfig {
            chain_id: 1,
            homestead_block: Some(0),
            dao_fork: Some(DaoConfig {
                block_number: 5,
                ..MAINNET_CONFIG.dao_fork.clone().unwrap()
            }),
            ..ChainConfig::default()
        },
        Network::EIP158ToByzantiumAt5 => ChainConfig {
            chain_id: 1,
            homestead_block: Some(0),
            tangerine_block: Some(0),
            spurious_block: Some(0),
            byzantium_block: Some(5),
            ..ChainConfig::default()
        },
        Network::ByzantiumToConstantinopleFixAt5 => ChainConfig {
            chain_id: 1,
            homestead_block: Some(0),
            tangerine_block: Some(0),
            spurious_block: Some(0),
            byzantium_block: Some(0),
            constantinople_block: Some(5),
            petersburg_block: Some(5),
            ..ChainConfig::default()
        },
        Network::BerlinToLondonAt5 => ChainConfig {
            chain_id: 1,
            homestead_block: Some(0),
            tangerine_block: Some(0),
            spurious_block: Some(0),
            byzantium_block: Some(0),
            constantinople_block: Some(0),
            petersburg_block: Some(0),
            istanbul_block: Some(0),
            muir_glacier_block: Some(0),
            berlin_block: Some(0),
            london_block: Some(5),
            ..ChainConfig::default()
        },
        Network::EIP2384 => ChainConfig {
            chain_id: 1,
            homestead_block: Some(0),
            tangerine_block: Some(0),
            spurious_block: Some(0),
            byzantium_block: Some(0),
            constantinople_block: Some(0),
            petersburg_block: Some(0),
            istanbul_block: Some(0),
            muir_glacier_block: Some(0),
            ..ChainConfig::default()
        },
    }
});

pub static DIFFICULTY_CONFIG: Lazy<HashMap<String, ChainConfig>> = Lazy::new(|| {
    hashmap! {
        "difficulty.json".to_string() => MAINNET_CONFIG.clone(),
        "difficultyByzantium.json".to_string() => NETWORK_CONFIG[&Network::Byzantium].clone(),
        "difficultyConstantinople.json".to_string() => NETWORK_CONFIG[&Network::Constantinople].clone(),
        "difficultyCustomMainNetwork.json".to_string() => MAINNET_CONFIG.clone(),
        "difficultyEIP2384_random_to20M.json".to_string() => NETWORK_CONFIG[&Network::EIP2384].clone(),
        "difficultyEIP2384_random.json".to_string() => NETWORK_CONFIG[&Network::EIP2384].clone(),
        "difficultyEIP2384.json".to_string() => NETWORK_CONFIG[&Network::EIP2384].clone(),
        "difficultyFrontier.json".to_string() => NETWORK_CONFIG[&Network::Frontier].clone(),
        "difficultyHomestead.json".to_string() => NETWORK_CONFIG[&Network::Homestead].clone(),
        "difficultyMainNetwork.json".to_string() => MAINNET_CONFIG.clone(),
        "difficultyRopsten.json".to_string() => ROPSTEN_CONFIG.clone(),
    }
});

#[derive(Deserialize, Educe)]
#[educe(Debug)]
pub struct AccountState {
    pub balance: U256,
    #[serde(deserialize_with = "deserialize_str_as_bytes")]
    #[educe(Debug(method = "write_hex_string"))]
    pub code: Bytes<'static>,
    pub nonce: U64,
    pub storage: HashMap<U256, U256>,
}

fn deserialize_str_as_u64<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: de::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;

    let d = if let Some(stripped) = s.strip_prefix("0x") {
        u64::from_str_radix(stripped, 16)
    } else {
        s.parse()
    }
    .map_err(|e| format!("{}/{}", e, s))
    .unwrap();

    Ok(d)
}

fn deserialize_str_as_u128<'de, D>(deserializer: D) -> Result<u128, D::Error>
where
    D: de::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;

    let d = if let Some(stripped) = s.strip_prefix("0x") {
        u128::from_str_radix(stripped, 16)
    } else {
        s.parse()
    }
    .map_err(|e| format!("{}/{}", e, s))
    .unwrap();

    Ok(d)
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DifficultyTest {
    /// Timestamp of a previous block
    #[serde(deserialize_with = "deserialize_str_as_u64")]
    parent_timestamp: u64,
    /// Difficulty of a previous block
    #[serde(deserialize_with = "deserialize_str_as_u128")]
    parent_difficulty: u128,
    /// Timestamp of a current block
    #[serde(deserialize_with = "deserialize_str_as_u64")]
    current_timestamp: u64,
    /// Number of a current block (previous block number = currentBlockNumber - 1)
    #[serde(deserialize_with = "deserialize_str_as_u64")]
    current_block_number: u64,
    /// Difficulty of a current block
    #[serde(deserialize_with = "deserialize_str_as_u128")]
    current_difficulty: u128,
    #[serde(default)]
    parent_uncles: Option<H256>,
}

#[derive(Debug, Deserialize)]
enum SealEngine {
    Ethash,
    NoProof,
}

#[derive(Deserialize, Educe)]
#[educe(Debug)]
#[serde(rename_all = "camelCase")]
struct BlockchainTest {
    #[serde(rename = "_info")]
    info: Info,
    seal_engine: SealEngine,
    network: Network,
    pre: HashMap<Address, AccountState>,
    #[serde(rename = "genesisRLP", deserialize_with = "deserialize_str_as_bytes")]
    #[educe(Debug(method = "write_hex_string"))]
    genesis_rlp: Bytes<'static>,
    blocks: Vec<Map<String, Value>>,
    #[serde(default)]
    post_state_hash: Option<H256>,
    #[serde(default)]
    post_state: Option<HashMap<Address, AccountState>>,
    lastblockhash: H256,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Info {
    pub comment: String,
    #[serde(rename = "filling-rpc-server")]
    pub filling_rpc_server: String,
    #[serde(rename = "filling-tool-version")]
    pub filling_tool_version: String,
    pub lllcversion: String,
    pub source: String,
    pub source_hash: String,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Status {
    Passed,
    Failed,
}

#[instrument]
async fn init_pre_state<'storage, S: State<'storage>>(
    pre: &HashMap<Address, AccountState>,
    state: &mut S,
) {
    for (address, j) in pre {
        let mut account = Account {
            balance: j.balance,
            nonce: j.nonce.as_u64(),

            ..Default::default()
        };

        if !j.code.is_empty() {
            account.incarnation = DEFAULT_INCARNATION;
            account.code_hash = keccak256(&*j.code);
            state
                .update_account_code(
                    *address,
                    account.incarnation,
                    account.code_hash,
                    j.code.clone(),
                )
                .await
                .unwrap();
        }

        state
            .update_account(*address, None, Some(account.clone()))
            .await
            .unwrap();

        for (&key, &value) in &j.storage {
            state
                .update_storage(
                    *address,
                    account.incarnation,
                    u256_to_h256(key),
                    H256::zero(),
                    u256_to_h256(value),
                )
                .await
                .unwrap();
        }
    }
}

#[derive(Educe, Deserialize)]
#[educe(Debug)]
#[serde(rename_all = "camelCase")]
struct BlockCommon {
    #[serde(default)]
    expect_exception: Option<String>,
    #[educe(Debug(method = "write_hex_string"))]
    #[serde(deserialize_with = "deserialize_str_as_bytes")]
    rlp: Bytes<'static>,
}

#[instrument(skip(block_common, blockchain))]
async fn run_block<'storage: 'state, 'state, S, C>(
    consensus: &C,
    block_common: &BlockCommon,
    blockchain: &mut Blockchain<'storage, 'state, S>,
) -> anyhow::Result<()>
where
    S: State<'storage>,
    C: Consensus,
{
    let block = rlp::decode::<Block>(&block_common.rlp)?;

    debug!("Running block {:?}", block);

    let check_state_root = true;

    blockchain
        .insert_block(consensus, block, check_state_root)
        .await?;

    Ok(())
}

#[instrument]
async fn post_check<'storage, S: State<'storage>>(
    state: &S,
    expected: &HashMap<Address, AccountState>,
) -> anyhow::Result<()> {
    let number_of_accounts = state.number_of_accounts().await.unwrap();
    let expected_number_of_accounts: u64 = expected.len().try_into().unwrap();
    if number_of_accounts != expected_number_of_accounts {
        bail!(
            "Account number mismatch: {} != {}",
            number_of_accounts,
            expected_number_of_accounts
        );
    }

    for (&address, expected_account_state) in expected {
        let account = state
            .read_account(address)
            .await
            .unwrap()
            .ok_or_else(|| anyhow!("Missing account {}", address))?;

        ensure!(
            account.balance == expected_account_state.balance,
            "Balance mismatch for {}:\n{} != {}",
            address,
            account.balance,
            expected_account_state.balance
        );

        ensure!(
            account.nonce == expected_account_state.nonce.as_u64(),
            "Nonce mismatch for {}:\n{} != {}",
            address,
            account.nonce,
            expected_account_state.nonce
        );

        let code = state.read_code(account.code_hash).await.unwrap();
        ensure!(
            code == expected_account_state.code,
            "Code mismatch for {}:\n{} != {}",
            address,
            hex::encode(&code),
            hex::encode(&expected_account_state.code)
        );

        let storage_size = state
            .storage_size(address, account.incarnation)
            .await
            .unwrap();

        let expected_storage_size: u64 = expected_account_state.storage.len().try_into().unwrap();
        ensure!(
            storage_size == expected_storage_size,
            "Storage size mismatch for {}:\n{} != {}",
            address,
            storage_size,
            expected_storage_size
        );

        for (&key, &expected_value) in &expected_account_state.storage {
            let actual_value = state
                .read_storage(address, account.incarnation, u256_to_h256(key))
                .await
                .unwrap();
            ensure!(
                actual_value == u256_to_h256(expected_value),
                "Storage mismatch for {} at {}:\n{} != {}",
                address,
                key,
                actual_value,
                expected_value
            );
        }
    }

    Ok(())
}

fn result_is_expected(
    got: anyhow::Result<()>,
    expected_exception: Option<String>,
) -> anyhow::Result<()> {
    if got.is_err() ^ expected_exception.is_some() {
        bail!("Unexpected result: {:?} != {:?}", expected_exception, got);
    }

    Ok(())
}

/// https://ethereum-tests.readthedocs.io/en/latest/test_types/blockchain_tests.html
#[instrument(skip(testdata))]
async fn blockchain_test(testdata: BlockchainTest, _: Option<ChainConfig>) -> anyhow::Result<()> {
    let genesis_block = rlp::decode::<Block>(&*testdata.genesis_rlp).unwrap();

    let mut state = InMemoryState::default();
    let config = NETWORK_CONFIG[&testdata.network].clone();

    let consensus = NoProof;

    init_pre_state(&testdata.pre, &mut state).await;

    let mut blockchain = Blockchain::new(&mut state, config, genesis_block)
        .await
        .unwrap();

    for block in &testdata.blocks {
        let block_common =
            serde_json::from_value::<BlockCommon>(Value::Object(block.clone())).unwrap();
        result_is_expected(
            run_block(&consensus, &block_common, &mut blockchain).await,
            block_common.expect_exception,
        )?;
    }

    if let Some(expected_hash) = testdata.post_state_hash {
        let state_root = state.state_root_hash().await.unwrap();

        ensure!(
            state_root == expected_hash,
            "postStateHash mismatch: {} != {}",
            state_root,
            expected_hash
        );

        trace!("PostStateHash verification OK");
    }

    if let Some(expected_state) = &testdata.post_state {
        post_check(&state, expected_state).await?;

        trace!("PostState verification OK");
    }

    Ok(())
}

#[derive(Debug, Deserialize)]
pub struct TransactionTest {
    #[serde(default)]
    pub hash: Option<String>,
    #[serde(default)]
    pub sender: Option<String>,
}

// https://ethereum-tests.readthedocs.io/en/latest/test_types/transaction_tests.html
#[instrument(skip(testdata))]
async fn transaction_test(
    testdata: HashMap<String, Value>,
    _: Option<ChainConfig>,
) -> anyhow::Result<()> {
    let txn = &hex::decode(
        testdata["rlp"]
            .as_str()
            .unwrap()
            .strip_prefix("0x")
            .unwrap(),
    )
    .map_err(anyhow::Error::new)
    .and_then(|v| Ok(rlp::decode::<akula::models::Transaction>(&v)?));

    for (key, tdvalue) in testdata {
        if key == "rlp" || key == "_info" {
            continue;
        }

        let t = serde_json::from_value::<TransactionTest>(tdvalue).unwrap();

        let valid = t.sender.is_some();

        match &txn {
            Err(e) => {
                if valid {
                    return Err(anyhow::Error::msg(format!("{:?}", e))
                        .context("Failed to decode valid transaction"));
                }
            }
            Ok(txn) => {
                let config = &NETWORK_CONFIG[&key.parse().unwrap()];

                if let Err(e) = pre_validate_transaction(txn, 0, config, None) {
                    if valid {
                        return Err(anyhow::Error::new(e).context("Validation error"));
                    } else {
                        continue;
                    }
                }

                match txn.recover_sender() {
                    Err(e) => {
                        if valid {
                            return Err(e.context("Failed to recover sender"));
                        }
                    }
                    Ok(sender) => {
                        if !valid {
                            bail!("Sender recovered for invalid transaction")
                        }

                        ensure!(
                            hex::encode(sender.0) == *t.sender.as_ref().unwrap(),
                            "Sender mismatch for {:?}: {:?} != {:?}",
                            t.hash.as_ref().unwrap(),
                            t.sender.as_ref().unwrap(),
                            sender
                        );
                    }
                }
            }
        }
    }

    Ok(())
}

#[instrument(skip(config))]
async fn difficulty_test(
    testdata: DifficultyTest,
    config: Option<ChainConfig>,
) -> anyhow::Result<()> {
    let parent_has_uncles = testdata
        .parent_uncles
        .map(|hash| hash != EMPTY_LIST_HASH)
        .unwrap_or(false);

    let calculated_difficulty = canonical_difficulty(
        testdata.current_block_number,
        testdata.current_timestamp,
        testdata.parent_difficulty.into(),
        testdata.parent_timestamp,
        parent_has_uncles,
        &config.unwrap(),
    );

    ensure!(
        calculated_difficulty.as_u128() == testdata.current_difficulty,
        "Difficulty mismatch for block {}\n{} != {}",
        testdata.current_block_number,
        calculated_difficulty,
        testdata.current_difficulty
    );

    Ok(())
}

#[instrument(skip(f, config))]
async fn run_test_file<Test, Fut>(
    path: &Path,
    test_names: &HashSet<String>,
    f: fn(Test, Option<ChainConfig>) -> Fut,
    config: Option<ChainConfig>,
) -> RunResults
where
    Fut: Future<Output = anyhow::Result<()>>,
    for<'de> Test: Deserialize<'de>,
{
    let j: HashMap<String, Test> = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();

    let mut out = RunResults::default();
    for (test_name, test) in j {
        if !test_names.is_empty() && !test_names.contains(&test_name) {
            continue;
        }

        debug!("Running test {}", test_name);
        out.push({
            if let Err(e) = (f)(test, config.clone()).await {
                error!("{}: {}", test_name, e);
                Status::Failed
            } else {
                Status::Passed
            }
        });
    }

    out
}

#[derive(StructOpt)]
#[structopt(name = "Consensus tests", about = "Run consensus tests against Akula.")]
pub struct Opt {
    /// Path to consensus tests
    #[structopt(long, env)]
    pub tests: PathBuf,
    #[structopt(long, env)]
    pub test_names: Vec<String>,
    #[structopt(long, env)]
    pub tokio_console: bool,
}

#[derive(Debug, Default)]
struct RunResults {
    passed: usize,
    failed: usize,
    skipped: usize,
}

impl RunResults {
    fn push(&mut self, result: Status) {
        match result {
            Status::Passed => {
                self.passed += 1;
            }
            Status::Failed => {
                self.failed += 1;
            }
        }
    }
}

impl AddAssign<RunResults> for RunResults {
    fn add_assign(&mut self, rhs: RunResults) {
        self.passed += rhs.passed;
        self.failed += rhs.failed;
        self.skipped += rhs.skipped;
    }
}

fn exclude_test(p: &Path, root: &Path) -> bool {
    for e in &*EXCLUDED_TESTS {
        if root.join(e) == p {
            return true;
        }
    }

    false
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opt = Opt::from_args();

    let filter = if std::env::var(EnvFilter::DEFAULT_ENV)
        .unwrap_or_default()
        .is_empty()
    {
        EnvFilter::new("akula=info")
    } else {
        EnvFilter::from_default_env()
    };
    let registry = tracing_subscriber::registry()
        // the `TasksLayer` can be used in combination with other `tracing` layers...
        .with(tracing_subscriber::fmt::layer().with_target(false));

    if opt.tokio_console {
        let (layer, server) = console_subscriber::TasksLayer::new();
        registry
            .with(filter.add_directive("tokio=trace".parse()?))
            .with(layer)
            .init();
        tokio::spawn(async move { server.serve().await.expect("server failed") });
    } else {
        registry.with(filter).init();
    }

    let root_dir = opt.tests;
    let test_names = opt.test_names.into_iter().collect();

    let mut res = RunResults::default();
    for (f, config) in &*DIFFICULTY_CONFIG {
        res += run_test_file(
            &root_dir.join(&*DIFFICULTY_DIR).join(f),
            &test_names,
            difficulty_test,
            Some(config.clone()),
        )
        .await;
    }

    let mut skipped = 0;
    for entry in walkdir::WalkDir::new(root_dir.join(&*BLOCKCHAIN_DIR))
        .into_iter()
        .filter_entry(|e| {
            if exclude_test(e.path(), &root_dir) {
                skipped += 1;
                return false;
            }

            true
        })
    {
        let e = entry.unwrap();

        if e.file_type().is_file() {
            res += run_test_file(e.path(), &test_names, blockchain_test, None).await;
        }
    }

    for entry in walkdir::WalkDir::new(root_dir.join(&*TRANSACTION_DIR))
        .into_iter()
        .filter_entry(|e| {
            if exclude_test(e.path(), &root_dir) {
                skipped += 1;
                return false;
            }

            true
        })
    {
        let e = entry.unwrap();

        if e.file_type().is_file() {
            res += run_test_file(e.path(), &test_names, transaction_test, None).await;
        }
    }

    res.skipped += skipped;
    println!("Ethereum Consensus Tests:\n{:?}", res);

    if res.failed > 0 {
        std::process::exit(1);
    }

    Ok(())
}
