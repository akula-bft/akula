#![feature(let_else)]
#![allow(clippy::suspicious_else_formatting)]
use akula::{consensus::*, crypto::keccak256, models::*, res::chainspec::*, *};
use anyhow::{bail, ensure, format_err};
use bytes::Bytes;
use clap::Parser;
use educe::Educe;
use expanded_pathbuf::ExpandedPathBuf;
use fastrlp::*;
use maplit::*;
use once_cell::sync::Lazy;
use serde::Deserialize;
use serde_json::{Map, Value};
use std::{
    collections::{HashMap, HashSet},
    convert::TryInto,
    fmt::Debug,
    ops::AddAssign,
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
    time::Instant,
};
use tokio::runtime::Builder;
use tracing::*;
use tracing_subscriber::{prelude::*, EnvFilter};

pub static DIFFICULTY_DIR: Lazy<PathBuf> = Lazy::new(|| Path::new("DifficultyTests").to_path_buf());
pub static BLOCKCHAIN_DIR: Lazy<PathBuf> = Lazy::new(|| Path::new("BlockchainTests").to_path_buf());
pub static TRANSACTION_DIR: Lazy<PathBuf> =
    Lazy::new(|| Path::new("TransactionTests").to_path_buf());

pub static IGNORED_TX_EXCEPTIONS: Lazy<HashSet<&'static str>> = Lazy::new(|| {
    hashset! {
        // This is not checked for now.
        "InvalidVRS",

        // Post-intrinsic gas calculation is part of execution, not pre-validation.
        "TR_IntrinsicGas",

        // Actually fix this when we have a new transaction type.
        "TR_TypeNotSupported",
    }
});

pub static EXCLUDED_TESTS: Lazy<Vec<PathBuf>> = Lazy::new(|| {
    vec![
        // Very slow tests
        BLOCKCHAIN_DIR
            .join("GeneralStateTests")
            .join("stTimeConsuming"),
        BLOCKCHAIN_DIR
            .join("GeneralStateTests")
            .join("VMTests")
            .join("vmPerformance"),
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
        // Edge case unlikely to be reached ever
        TRANSACTION_DIR
            .join("ttNonce")
            .join("TransactionWithHighNonce64Minus1.json"),
        // Should be fixed in rlp crate
        TRANSACTION_DIR
            .join("ttWrongRLP")
            .join("TRANSCT__RandomByteAtTheEnd.json"),
        TRANSACTION_DIR
            .join("ttWrongRLP")
            .join("TRANSCT__RandomByteAtRLP_9.json"),
        TRANSACTION_DIR
            .join("ttWrongRLP")
            .join("TRANSCT__ZeroByteAtRLP_9.json"),
    ]
    .into_iter()
    // Will fix these later
    .chain(
        [
            "DifficultyIsZero.json",
            "badTimestamp.json",
            "timeDiff0.json",
            "wrongDifficulty.json",
        ]
        .into_iter()
        .map(|t| {
            BLOCKCHAIN_DIR
                .join("InvalidBlocks")
                .join("bcInvalidHeaderTest")
                .join(t)
        }),
    )
    .chain([BLOCKCHAIN_DIR
        .join("InvalidBlocks")
        .join("bcUncleHeaderValidity")
        .join("gasLimitTooLowExactBound.json")])
    .collect()
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
    ArrowGlacier,
    ArrowGlacierToMergeAtDiffC0000,
    Merge,
}

impl FromStr for Network {
    type Err = String;

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
            "ArrowGlacier" => Self::ArrowGlacier,
            "ArrowGlacierToMergeAtDiffC0000" => Self::ArrowGlacierToMergeAtDiffC0000,
            "Merge" => Self::Merge,
            _ => return Err(s.to_string()),
        })
    }
}

fn testconfig(name: Network, upgrades: Upgrades, dao_block: Option<BlockNumber>) -> ChainSpec {
    let mut spec = MAINNET.clone();
    spec.name = format!("{:?}", name);
    spec.consensus.eip1559_block = upgrades.london;
    let SealVerificationParams::Beacon { block_reward, .. } = &mut spec.consensus.seal_verification else { unreachable!() };
    spec.upgrades = upgrades;

    block_reward.clear();
    block_reward.insert(BlockNumber(0), (5 * ETHER).as_u256());
    if let Some(block) = spec.upgrades.byzantium {
        block_reward.insert(block, (3 * ETHER).as_u256());
    }
    if let Some(block) = spec.upgrades.constantinople {
        block_reward.insert(block, (2 * ETHER).as_u256());
    }
    if let Some(block) = spec.upgrades.paris {
        block_reward.insert(block, U256::ZERO);
    }

    let mainnet_dao_fork_block_num = BlockNumber(1_920_000);
    let dao_data = spec.balances.remove(&mainnet_dao_fork_block_num).unwrap();
    spec.balances.clear();
    if let Some(dao_block) = dao_block {
        spec.balances.insert(dao_block, dao_data);
    }

    spec
}

static NETWORK_CONFIG: Lazy<HashMap<Network, ChainSpec>> = Lazy::new(|| {
    vec![
        (Network::Frontier, Upgrades::default(), None),
        (
            Network::Homestead,
            Upgrades {
                homestead: Some(0.into()),
                ..Default::default()
            },
            None,
        ),
        (
            Network::EIP150,
            Upgrades {
                homestead: Some(0.into()),
                tangerine: Some(0.into()),
                ..Default::default()
            },
            None,
        ),
        (
            Network::EIP158,
            Upgrades {
                homestead: Some(0.into()),
                tangerine: Some(0.into()),
                spurious: Some(0.into()),
                ..Default::default()
            },
            None,
        ),
        (
            Network::Byzantium,
            Upgrades {
                homestead: Some(0.into()),
                tangerine: Some(0.into()),
                spurious: Some(0.into()),
                byzantium: Some(0.into()),
                ..Default::default()
            },
            None,
        ),
        (
            Network::Constantinople,
            Upgrades {
                homestead: Some(0.into()),
                tangerine: Some(0.into()),
                spurious: Some(0.into()),
                byzantium: Some(0.into()),
                constantinople: Some(0.into()),
                ..Default::default()
            },
            None,
        ),
        (
            Network::ConstantinopleFix,
            Upgrades {
                homestead: Some(0.into()),
                tangerine: Some(0.into()),
                spurious: Some(0.into()),
                byzantium: Some(0.into()),
                constantinople: Some(0.into()),
                petersburg: Some(0.into()),
                ..Default::default()
            },
            None,
        ),
        (
            Network::Istanbul,
            Upgrades {
                homestead: Some(0.into()),
                tangerine: Some(0.into()),
                spurious: Some(0.into()),
                byzantium: Some(0.into()),
                constantinople: Some(0.into()),
                petersburg: Some(0.into()),
                istanbul: Some(0.into()),
                ..Default::default()
            },
            None,
        ),
        (
            Network::Berlin,
            Upgrades {
                homestead: Some(0.into()),
                tangerine: Some(0.into()),
                spurious: Some(0.into()),
                byzantium: Some(0.into()),
                constantinople: Some(0.into()),
                petersburg: Some(0.into()),
                istanbul: Some(0.into()),
                berlin: Some(0.into()),
                ..Default::default()
            },
            None,
        ),
        (
            Network::London,
            Upgrades {
                homestead: Some(0.into()),
                tangerine: Some(0.into()),
                spurious: Some(0.into()),
                byzantium: Some(0.into()),
                constantinople: Some(0.into()),
                petersburg: Some(0.into()),
                istanbul: Some(0.into()),
                berlin: Some(0.into()),
                london: Some(0.into()),
                ..Default::default()
            },
            None,
        ),
        (
            Network::FrontierToHomesteadAt5,
            Upgrades {
                homestead: Some(5.into()),
                ..Default::default()
            },
            None,
        ),
        (
            Network::HomesteadToEIP150At5,
            Upgrades {
                homestead: Some(0.into()),
                tangerine: Some(5.into()),
                ..Default::default()
            },
            None,
        ),
        (
            Network::HomesteadToDaoAt5,
            Upgrades {
                homestead: Some(0.into()),
                ..Default::default()
            },
            Some(5.into()),
        ),
        (
            Network::EIP158ToByzantiumAt5,
            Upgrades {
                homestead: Some(0.into()),
                tangerine: Some(0.into()),
                spurious: Some(0.into()),
                byzantium: Some(5.into()),
                ..Default::default()
            },
            None,
        ),
        (
            Network::ByzantiumToConstantinopleFixAt5,
            Upgrades {
                homestead: Some(0.into()),
                tangerine: Some(0.into()),
                spurious: Some(0.into()),
                byzantium: Some(0.into()),
                constantinople: Some(5.into()),
                petersburg: Some(5.into()),
                ..Default::default()
            },
            None,
        ),
        (
            Network::BerlinToLondonAt5,
            Upgrades {
                homestead: Some(0.into()),
                tangerine: Some(0.into()),
                spurious: Some(0.into()),
                byzantium: Some(0.into()),
                constantinople: Some(0.into()),
                petersburg: Some(0.into()),
                istanbul: Some(0.into()),
                berlin: Some(0.into()),
                london: Some(5.into()),
                ..Default::default()
            },
            None,
        ),
        (
            Network::EIP2384,
            Upgrades {
                homestead: Some(0.into()),
                tangerine: Some(0.into()),
                spurious: Some(0.into()),
                byzantium: Some(0.into()),
                constantinople: Some(0.into()),
                petersburg: Some(0.into()),
                istanbul: Some(0.into()),
                ..Default::default()
            },
            None,
        ),
        (
            Network::ArrowGlacier,
            Upgrades {
                homestead: Some(0.into()),
                tangerine: Some(0.into()),
                spurious: Some(0.into()),
                byzantium: Some(0.into()),
                constantinople: Some(0.into()),
                petersburg: Some(0.into()),
                istanbul: Some(0.into()),
                berlin: Some(0.into()),
                london: Some(0.into()),
                ..Default::default()
            },
            None,
        ),
    ]
    .into_iter()
    .map(|(network, upgrades, dao_block)| (network, testconfig(network, upgrades, dao_block)))
    .collect()
});

#[derive(Deserialize, Educe)]
#[educe(Debug)]
pub struct AccountState {
    pub balance: U256,
    #[serde(with = "hexbytes")]
    #[educe(Debug(method = "write_hex_string"))]
    pub code: Bytes,
    pub nonce: U64,
    pub storage: HashMap<U256, U256>,
}

#[derive(Debug, Deserialize)]
enum SealEngine {
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
    #[serde(rename = "genesisRLP", with = "hexbytes")]
    #[educe(Debug(method = "write_hex_string"))]
    genesis_rlp: Bytes,
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
fn init_pre_state<S>(pre: &HashMap<Address, AccountState>, state: &mut S)
where
    S: State,
{
    for (address, j) in pre {
        let mut account = Account {
            balance: j.balance,
            nonce: j.nonce.as_u64(),

            ..Default::default()
        };

        if !j.code.is_empty() {
            account.code_hash = keccak256(&*j.code);
            state
                .update_code(account.code_hash, j.code.clone())
                .unwrap();
        }

        state.update_account(*address, None, Some(account));

        for (&key, &value) in &j.storage {
            state
                .update_storage(*address, key, U256::ZERO, value)
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
    #[serde(with = "hexbytes")]
    rlp: Bytes,
}

#[instrument(skip(block_common, blockchain))]
fn run_block<'state>(
    block_common: &BlockCommon,
    blockchain: &mut Blockchain<'state>,
) -> anyhow::Result<()> {
    let block = <Block as Decodable>::decode(&mut &*block_common.rlp)?;

    debug!("Running block {:?}", block);

    let check_state_root = true;

    blockchain.insert_block(block, check_state_root)?;

    Ok(())
}

#[instrument]
fn post_check(
    state: &InMemoryState,
    expected: &HashMap<Address, AccountState>,
) -> anyhow::Result<()> {
    let number_of_accounts = state.number_of_accounts();
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
            .unwrap()
            .ok_or_else(|| format_err!("Missing account {}", address))?;

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

        let code = state.read_code(account.code_hash).unwrap();
        ensure!(
            code == expected_account_state.code,
            "Code mismatch for {}:\n{} != {}",
            address,
            hex::encode(&code),
            hex::encode(&expected_account_state.code)
        );

        let storage_size = state.storage_size(address);

        let expected_storage_size: u64 = expected_account_state.storage.len().try_into().unwrap();
        ensure!(
            storage_size == expected_storage_size,
            "Storage size mismatch for {}:\n{} != {}",
            address,
            storage_size,
            expected_storage_size
        );

        for (&key, &expected_value) in &expected_account_state.storage {
            let actual_value = state.read_storage(address, key).unwrap();
            ensure!(
                actual_value == expected_value,
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

/// <https://ethereum-tests.readthedocs.io/en/latest/test_types/blockchain_tests.html>
#[instrument(skip(testdata))]
fn blockchain_test(testdata: BlockchainTest) -> anyhow::Result<()> {
    let genesis_block = <Block as Decodable>::decode(&mut &*testdata.genesis_rlp).unwrap();

    let mut state = InMemoryState::default();
    let Some(config) = NETWORK_CONFIG.get(&testdata.network).cloned() else {
        warn!("Network config {:?} not found, skipping test", testdata.network);
        return Ok(());
    };

    init_pre_state(&testdata.pre, &mut state);

    let mut blockchain = Blockchain::new(&mut state, config, genesis_block).unwrap();

    for block in &testdata.blocks {
        let block_common =
            serde_json::from_value::<BlockCommon>(Value::Object(block.clone())).unwrap();
        result_is_expected(
            run_block(&block_common, &mut blockchain),
            block_common.expect_exception,
        )?;
    }

    if let Some(expected_hash) = testdata.post_state_hash {
        let state_root = state.state_root_hash();

        ensure!(
            state_root == expected_hash,
            "postStateHash mismatch: {} != {}",
            state_root,
            expected_hash
        );

        trace!("PostStateHash verification OK");
    }

    if let Some(expected_state) = &testdata.post_state {
        post_check(&state, expected_state)?;

        trace!("PostState verification OK");
    }

    Ok(())
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum TransactionTestResult {
    Correct { hash: H256, sender: Address },
    Incorrect { exception: String },
}

#[derive(Debug, Deserialize)]
pub struct TransactionTest {
    pub result: HashMap<String, TransactionTestResult>,
    #[serde(with = "hexbytes")]
    pub txbytes: Bytes,
}

// https://ethereum-tests.readthedocs.io/en/latest/test_types/transaction_tests.html
#[instrument(skip(testdata))]
fn transaction_test(testdata: TransactionTest) -> anyhow::Result<()> {
    let txn = <akula::models::MessageWithSignature as Decodable>::decode(&mut &*testdata.txbytes);

    for (key, t) in testdata.result {
        match (&txn, t) {
            (Err(e), TransactionTestResult::Correct { .. }) => {
                return Err(anyhow::Error::msg(format!("{:?}", e))
                    .context("Failed to decode valid transaction"));
            }
            (Ok(txn), t) => {
                let key = key.parse().unwrap();
                let Some(config) = &NETWORK_CONFIG.get(&key) else {
                    warn!("Network config {key:?} not found, skipping test");
                    continue;
                };

                if let Err(e) = pre_validate_transaction(txn, config.params.chain_id, None) {
                    match t {
                        TransactionTestResult::Correct { hash, sender } => {
                            return Err(format_err!(
                                "Unexpected validation error (tx hash {:?}, sender {:?}): {:?}",
                                hash,
                                sender,
                                e
                            ));
                        }
                        TransactionTestResult::Incorrect { .. } => {
                            continue;
                        }
                    }
                }

                match (txn.recover_sender(), t) {
                    (Err(e), TransactionTestResult::Correct { hash, sender }) => {
                        return Err(e.context(format!(
                            "Failed to recover sender (tx hash {:?}, sender {:?})",
                            hash, sender
                        )));
                    }
                    (Ok(_), TransactionTestResult::Incorrect { exception }) => {
                        if !IGNORED_TX_EXCEPTIONS.contains(&*exception) {
                            bail!(
                                "Sender recovered for invalid transaction (exception {})",
                                exception
                            )
                        }
                    }
                    (Ok(recovered_sender), TransactionTestResult::Correct { sender, hash }) => {
                        ensure!(
                            recovered_sender == sender,
                            "Sender mismatch for {:?}: {:?} != {:?}",
                            hash,
                            sender,
                            recovered_sender
                        );
                    }
                    (Err(_), TransactionTestResult::Incorrect { .. }) => {}
                }
            }
            _ => continue,
        }
    }

    Ok(())
}

#[instrument(skip(f))]
fn run_test_file<Test>(
    path: &Path,
    test_names: &HashSet<String>,
    f: fn(Test) -> anyhow::Result<()>,
) -> RunResults
where
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
            if let Err(e) = (f)(test) {
                error!("{}: {}: {}", path.to_string_lossy(), test_name, e);
                Status::Failed
            } else {
                Status::Passed
            }
        });
    }

    out
}

#[derive(Parser)]
#[clap(name = "Consensus tests", about = "Run consensus tests against Akula.")]
pub struct Opt {
    /// Path to consensus tests
    #[clap(long)]
    pub tests: ExpandedPathBuf,
    #[clap(long)]
    pub test_names: Vec<String>,
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

async fn run() {
    let now = Instant::now();

    let opt = Opt::parse();

    let env_filter = if std::env::var(EnvFilter::DEFAULT_ENV)
        .unwrap_or_default()
        .is_empty()
    {
        EnvFilter::new("akula=info")
    } else {
        EnvFilter::from_default_env()
    };
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_target(false))
        .with(env_filter)
        .init();

    let root_dir = opt.tests;
    let test_names = Arc::new(opt.test_names.into_iter().collect());

    let mut tasks = Vec::new();
    let mut res = RunResults::default();

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
            let p = e.into_path();
            let test_names = Arc::clone(&test_names);
            tasks.push(tokio::spawn(async move {
                run_test_file(p.as_path(), &test_names, blockchain_test)
            }));
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
            let p = e.into_path();
            let test_names = Arc::clone(&test_names);
            tasks.push(tokio::spawn(async move {
                run_test_file(p.as_path(), &test_names, transaction_test)
            }));
        }
    }

    for task in tasks {
        res += task.await.unwrap();
    }

    res.skipped += skipped;
    println!(
        "Ethereum Consensus Tests:\n{:?}\nElapsed {:?}",
        res,
        now.elapsed()
    );

    if res.failed > 0 {
        std::process::exit(1);
    }
}

fn main() {
    Builder::new_multi_thread()
        .enable_all()
        .thread_stack_size(128 * 1024 * 1024)
        .build()
        .unwrap()
        .block_on(run());
}
