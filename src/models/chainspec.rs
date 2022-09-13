use crate::{consensus::BeneficiaryFunction, models::*, util::*};
use bytes::Bytes;
use serde::*;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    time::Duration,
};
use sha3::{Digest, Keccak256};

type NodeUrl = String;

#[derive(Debug, PartialEq, Eq)]
pub struct BlockExecutionSpec {
    pub revision: Revision,
    pub active_transitions: HashSet<Revision>,
    pub params: Params,
    pub consensus: ConsensusParams,
    pub system_contract_changes: HashMap<Address, Contract>,
    pub balance_changes: HashMap<Address, U256>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ChainSpec {
    pub name: String,
    pub consensus: ConsensusParams,
    #[serde(default)]
    pub upgrades: Upgrades,
    pub params: Params,
    pub genesis: Genesis,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub contracts: BTreeMap<BlockNumber, HashMap<Address, Contract>>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub balances: BTreeMap<BlockNumber, HashMap<Address, U256>>,
    pub p2p: P2PParams,
}

impl ChainSpec {
    pub fn collect_block_spec(&self, block_number: impl Into<BlockNumber>) -> BlockExecutionSpec {
        let block_number = block_number.into();
        let mut revision = Revision::Frontier;
        let mut active_transitions = HashSet::new();
        for (fork, r) in [
            (self.upgrades.paris, Revision::Paris),
            (self.upgrades.london, Revision::London),
            (self.upgrades.berlin, Revision::Berlin),
            (self.upgrades.istanbul, Revision::Istanbul),
            (self.upgrades.petersburg, Revision::Petersburg),
            (self.upgrades.constantinople, Revision::Constantinople),
            (self.upgrades.byzantium, Revision::Byzantium),
            (self.upgrades.spurious, Revision::Spurious),
            (self.upgrades.tangerine, Revision::Tangerine),
            (self.upgrades.homestead, Revision::Homestead),
        ] {
            if let Some(fork_block) = fork {
                if block_number >= fork_block {
                    if block_number == fork_block {
                        active_transitions.insert(r);
                    }
                    if revision == Revision::Frontier {
                        revision = r;
                    }

                    break;
                }
            }
        }

        BlockExecutionSpec {
            revision,
            active_transitions,
            params: self.params.clone(),
            consensus: self.consensus.clone(),
            system_contract_changes: self.contracts.iter().fold(
                HashMap::new(),
                |mut acc, (bn, contracts)| {
                    if block_number >= *bn {
                        for (addr, contract) in contracts {
                            acc.insert(*addr, contract.clone());
                        }
                    }

                    acc
                },
            ),
            balance_changes: self
                .balances
                .get(&block_number)
                .cloned()
                .unwrap_or_default(),
        }
    }

    pub fn gather_forks(&self) -> BTreeSet<BlockNumber> {
        let mut forks = [
            self.upgrades.homestead,
            self.upgrades.tangerine,
            self.upgrades.spurious,
            self.upgrades.byzantium,
            self.upgrades.constantinople,
            self.upgrades.petersburg,
            self.upgrades.istanbul,
            self.upgrades.berlin,
            self.upgrades.london,
            // upgrades for parlia,
            self.upgrades.ramanujan,
            self.upgrades.niels,
            self.upgrades.mirrorsync,
            self.upgrades.bruno,
            self.upgrades.euler,
            ]
        .iter()
        .copied()
        .flatten()
        .chain(self.consensus.eip1559_block)
        .chain(self.consensus.seal_verification.gather_forks())
        .chain(self.contracts.keys().copied())
        .chain(self.balances.keys().copied())
        .chain(self.params.additional_forks.iter().copied())
        .collect::<BTreeSet<BlockNumber>>();

        forks.remove(&BlockNumber(0));

        forks
    }

    pub fn try_get_code_with_hash(&self, block_number: BlockNumber, address: &Address) -> Option<(H256, Bytes)> {
        if let Some(contracts) = self.contracts.get(&block_number) {
            if let Some(contract) = contracts.get(&address) {
                if let Contract::Contract { code } = contract {
                    return Some((H256::from_slice(&Keccak256::digest(&code)[..]), code.clone()));
                }
            }
        }
        None
    }

    pub fn is_on_ramanujan(&self, number: &BlockNumber) -> bool {
        is_on_forked(self.upgrades.ramanujan, number)
    }

    pub fn is_on_niels(&self, number: &BlockNumber) -> bool {
        is_on_forked(self.upgrades.niels, number)
    }

    pub fn is_on_mirror_sync(&self, number: &BlockNumber) -> bool {
        is_on_forked(self.upgrades.mirrorsync, number)
    }

    pub fn is_on_bruno(&self, number: &BlockNumber) -> bool {
        is_on_forked(self.upgrades.bruno, number)
    }

    pub fn is_on_euler(&self, number: &BlockNumber) -> bool {
        is_on_forked(self.upgrades.euler, number)
    }

    pub fn is_on_gibbs(&self, number: &BlockNumber) -> bool {
        is_on_forked(self.upgrades.gibbs, number)
    }

    pub fn is_ramanujan(&self, number: &BlockNumber) -> bool {
        is_forked(self.upgrades.ramanujan, number)
    }

    pub fn is_niels(&self, number: &BlockNumber) -> bool {
        is_forked(self.upgrades.niels, number)
    }

    pub fn is_mirror_sync(&self, number: &BlockNumber) -> bool {
        is_forked(self.upgrades.mirrorsync, number)
    }

    pub fn is_bruno(&self, number: &BlockNumber) -> bool {
        is_forked(self.upgrades.bruno, number)
    }

    pub fn is_euler(&self, number: &BlockNumber) -> bool {
        is_forked(self.upgrades.euler, number)
    }

    pub fn is_gibbs(&self, number: &BlockNumber) -> bool {
        is_forked(self.upgrades.gibbs, number)
    }
}

/// is_forked returns whether a fork scheduled at block s is active at the given head block.
#[inline]
pub fn is_forked(forked_op: Option<BlockNumber>, current: &BlockNumber) -> bool {
    match forked_op {
        None => {
            false
        }
        Some(forked) => {
            *current >= forked
        }
    }
}

/// is_on_forked returns whether a fork is at target block number.
#[inline]
pub fn is_on_forked(fork_op: Option<BlockNumber>, current: &BlockNumber) -> bool {
    match fork_op {
        None => {
            false
        }
        Some(fork) => {
            *current == fork
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DifficultyBomb {
    pub delays: BTreeMap<BlockNumber, BlockNumber>,
}

impl DifficultyBomb {
    pub fn get_delay_to(&self, block_number: BlockNumber) -> BlockNumber {
        self.delays
            .iter()
            .filter_map(|(&activation, &delay_to)| {
                if block_number >= activation {
                    Some(delay_to)
                } else {
                    None
                }
            })
            .last()
            .unwrap_or(BlockNumber(0))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsensusParams {
    pub seal_verification: SealVerificationParams,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "::serde_with::rust::unwrap_or_skip"
    )]
    pub eip1559_block: Option<BlockNumber>,
}

impl ConsensusParams {

    pub fn is_parlia(&self) -> bool {
        match self.seal_verification {
            SealVerificationParams::Parlia { .. } => {
                true
            },
            _ => false
        }
    }
}

pub fn switch_is_active(switch: Option<BlockNumber>, block_number: BlockNumber) -> bool {
    block_number >= switch.unwrap_or(BlockNumber(u64::MAX))
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SealVerificationParams {
    Clique {
        #[serde(with = "duration_as_millis")]
        period: Duration,
        epoch: u64,
    },
    Beacon {
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            with = "::serde_with::rust::unwrap_or_skip"
        )]
        terminal_total_difficulty: Option<U256>,
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            with = "::serde_with::rust::unwrap_or_skip"
        )]
        terminal_block_hash: Option<H256>,
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            with = "::serde_with::rust::unwrap_or_skip"
        )]
        terminal_block_number: Option<BlockNumber>,
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            with = "::serde_with::rust::unwrap_or_skip"
        )]
        since: Option<BlockNumber>,
        #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
        block_reward: BTreeMap<BlockNumber, U256>,
        #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
        beneficiary: BTreeMap<BlockNumber, BeneficiaryFunction>,
    },
    Parlia {
        /// Number of seconds between blocks to enforce
        period: u64,
        /// Epoch length to update validatorSet
        epoch: u64,
    },
}

impl SealVerificationParams {
    pub fn gather_forks(&self) -> BTreeSet<BlockNumber> {
        match self {
            SealVerificationParams::Beacon { .. } => BTreeSet::new(),
            _ => BTreeSet::new(),
        }
    }
}

// deserialize_str_as_u64
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Upgrades {
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "::serde_with::rust::unwrap_or_skip"
    )]
    pub homestead: Option<BlockNumber>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "::serde_with::rust::unwrap_or_skip"
    )]
    pub tangerine: Option<BlockNumber>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "::serde_with::rust::unwrap_or_skip"
    )]
    pub spurious: Option<BlockNumber>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "::serde_with::rust::unwrap_or_skip"
    )]
    pub byzantium: Option<BlockNumber>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "::serde_with::rust::unwrap_or_skip"
    )]
    pub constantinople: Option<BlockNumber>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "::serde_with::rust::unwrap_or_skip"
    )]
    pub petersburg: Option<BlockNumber>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "::serde_with::rust::unwrap_or_skip"
    )]
    pub istanbul: Option<BlockNumber>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "::serde_with::rust::unwrap_or_skip"
    )]
    pub berlin: Option<BlockNumber>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "::serde_with::rust::unwrap_or_skip"
    )]
    pub london: Option<BlockNumber>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "::serde_with::rust::unwrap_or_skip"
    )]
    pub paris: Option<BlockNumber>,

    /// bsc forks starts
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "::serde_with::rust::unwrap_or_skip"
    )]
    pub ramanujan: Option<BlockNumber>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "::serde_with::rust::unwrap_or_skip"
    )]
    pub niels: Option<BlockNumber>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "::serde_with::rust::unwrap_or_skip"
    )]
    pub mirrorsync: Option<BlockNumber>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "::serde_with::rust::unwrap_or_skip"
    )]
    pub bruno: Option<BlockNumber>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "::serde_with::rust::unwrap_or_skip"
    )]
    pub euler: Option<BlockNumber>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "::serde_with::rust::unwrap_or_skip"
    )]
    pub gibbs: Option<BlockNumber>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Params {
    pub chain_id: ChainId,
    pub network_id: NetworkId,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub additional_forks: BTreeSet<BlockNumber>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum BlockScore {
    NoTurn = 1,
    InTurn = 2,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Seal {
    Ethash {
        #[serde(with = "hexbytes")]
        vanity: Bytes,
        difficulty: U256,
        nonce: H64,
        mix_hash: H256,
    },
    Parlia {
        vanity: H256,
        score: BlockScore,
        signers: Vec<Address>,
    },
    Clique {
        vanity: H256,
        score: BlockScore,
        signers: Vec<Address>,
    },
}

impl Seal {
    pub fn difficulty(&self) -> U256 {
        match self {
            Seal::Ethash { difficulty, .. } => *difficulty,
            Seal::Parlia { score, .. } => (*score as u8).into(),
            Seal::Clique { score, .. } => (*score as u8).into(),
        }
    }

    pub fn extra_data(&self) -> Bytes {
        match self {
            Seal::Ethash { vanity, .. } => vanity.clone(),
            Seal::Parlia {
                vanity, signers, ..
            } => {
                let mut v = Vec::new();
                v.extend_from_slice(vanity.as_bytes());
                for signer in signers {
                    v.extend_from_slice(signer.as_bytes());
                }
                v.extend_from_slice(&[0; 65]);
                v.into()
            }
            Seal::Clique {
                vanity, signers, ..
            } => {
                let mut v = Vec::new();
                v.extend_from_slice(vanity.as_bytes());
                for signer in signers {
                    v.extend_from_slice(signer.as_bytes());
                }
                v.extend_from_slice(&[0; 65]);
                v.into()
            }
        }
    }

    pub fn mix_hash(&self) -> H256 {
        match self {
            Seal::Ethash { mix_hash, .. } => *mix_hash,
            _ => H256::zero(),
        }
    }

    pub fn nonce(&self) -> H64 {
        match self {
            Seal::Ethash { nonce, .. } => *nonce,
            _ => H64::zero(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Genesis {
    pub number: BlockNumber,
    pub author: Address,
    pub gas_limit: u64,
    pub timestamp: u64,
    pub seal: Seal,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "::serde_with::rust::unwrap_or_skip"
    )]
    pub base_fee_per_gas: Option<U256>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Contract {
    Contract {
        #[serde(with = "hexbytes")]
        code: Bytes,
    },
    Precompile(Precompile),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ModExpVersion {
    ModExp198,
    ModExp2565,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Precompile {
    EcRecover { base: u64, word: u64 },
    Sha256 { base: u64, word: u64 },
    Ripemd160 { base: u64, word: u64 },
    Identity { base: u64, word: u64 },
    ModExp { version: ModExpVersion },
    AltBn128Add { price: u64 },
    AltBn128Mul { price: u64 },
    AltBn128Pairing { base: u64, pair: u64 },
    Blake2F { gas_per_round: u64 },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct P2PParams {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub bootnodes: Vec<NodeUrl>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "::serde_with::rust::unwrap_or_skip"
    )]
    pub dns: Option<String>,
}

fn deserialize_str_as_u64<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: de::Deserializer<'de>,
{
    U64::deserialize(deserializer).map(|num| num.as_u64())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::res::chainspec::*;
    use hex_literal::hex;
    use maplit::*;

    #[test]
    fn load_chainspec() {
        assert_eq!(
            ChainSpec {
                name: "Rinkeby".into(),
                consensus: ConsensusParams {
                    seal_verification: SealVerificationParams::Clique {
                        period: Duration::from_millis(15),
                        epoch: 30_000,
                    },
                    eip1559_block: Some(8897988.into()),
                },
                upgrades: Upgrades {
                    homestead: Some(1.into()),
                    tangerine: Some(2.into()),
                    spurious: Some(3.into()),
                    byzantium: Some(1035301.into()),
                    constantinople: Some(3660663.into()),
                    petersburg: Some(4321234.into()),
                    istanbul: Some(5435345.into()),
                    berlin: Some(8290928.into()),
                    london: Some(8897988.into()),
                    paris: None,
                    ramanujan: None,
                    niels: None,
                    mirrorsync: None,
                    bruno: None,
                    euler: None,
                    gibbs: None
                },
                params: Params {
                    chain_id: ChainId(4),
                    network_id: NetworkId(4),
                    additional_forks: BTreeSet::new(),
                },
                genesis: Genesis {
                    number: BlockNumber(0),
                    author: hex!("0000000000000000000000000000000000000000").into(),
                    gas_limit: 0x47b760,
                    timestamp: 0x58ee40ba,
                    base_fee_per_gas: None,
                    seal: Seal::Clique {
                        vanity: hex!(
                            "52657370656374206d7920617574686f7269746168207e452e436172746d616e"
                        )
                        .into(),
                        score: BlockScore::NoTurn,
                        signers: vec![
                            hex!("42eb768f2244c8811c63729a21a3569731535f06").into(),
                            hex!("7ffc57839b00206d1ad20c69a1981b489f772031").into(),
                            hex!("b279182d99e65703f0076e4812653aab85fca0f0").into(),
                        ],
                    },
                },
                contracts: Default::default(),
                balances: btreemap! {
                    0.into() => (0x00..=0xff)
                    .map(|address| (Address::from_low_u64_be(address), 1u64.as_u256()))
                    .chain(vec![(
                        Address::from(hex!("31b98d14007bdee637298086988a0bbd31184523")),
                        U256::from_be_bytes(hex!(
                            "0200000000000000000000000000000000000000000000000000000000000000"
                        )),
                    )].into_iter()).collect::<HashMap<Address, U256>>(),
                },
                p2p: P2PParams {
                    bootnodes: vec![
                        "enode://a24ac7c5484ef4ed0c5eb2d36620ba4e4aa13b8c84684e1b4aab0cebea2ae45cb4d375b77eab56516d34bfbd3c1a833fc51296ff084b770b94fb9028c4d25ccf@52.169.42.101:30303",
                        "enode://343149e4feefa15d882d9fe4ac7d88f885bd05ebb735e547f12e12080a9fa07c8014ca6fd7f373123488102fe5e34111f8509cf0b7de3f5b44339c9f25e87cb8@52.3.158.184:30303",
                        "enode://b6b28890b006743680c52e64e0d16db57f28124885595fa03a562be1d2bf0f3a1da297d56b13da25fb992888fd556d4c1a27b1f39d531bde7de1921c90061cc6@159.89.28.211:30303",
                    ].into_iter().map(ToString::to_string).collect(),
                    dns: Some("all.rinkeby.ethdisco.net".to_owned()),
                }
            },
            *RINKEBY,
        );
    }

    #[test]
    fn distinct_block_numbers() {
        assert_eq!(
            MAINNET.gather_forks(),
            vec![
                1_150_000, 1_920_000, 2_463_000, 2_675_000, 4_370_000, 7_280_000, 9_069_000,
                9_200_000, 12_244_000, 12_965_000, 13_773_000, 15_050_000
            ]
            .into_iter()
            .map(BlockNumber)
            .collect()
        );
    }
}
