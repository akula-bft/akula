use crate::{models::*, util::*};
use bytes::Bytes;
use evmodin::Revision;
use serde::*;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    time::Duration,
};

type NodeUrl = String;

#[derive(Debug, PartialEq)]
pub struct BlockExecutionSpec {
    pub revision: Revision,
    pub active_transitions: HashSet<Revision>,
    pub params: Params,
    pub system_contract_changes: HashMap<Address, Contract>,
    pub balance_changes: HashMap<Address, U256>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
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
        ]
        .iter()
        .copied()
        .flatten()
        .chain(self.consensus.eip1559_block)
        .chain(self.consensus.seal_verification.gather_forks())
        .chain(self.contracts.keys().copied())
        .chain(self.balances.keys().copied())
        .collect::<BTreeSet<BlockNumber>>();

        forks.remove(&BlockNumber(0));

        forks
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
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

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ConsensusParams {
    pub seal_verification: SealVerificationParams,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "::serde_with::rust::unwrap_or_skip"
    )]
    pub eip1559_block: Option<BlockNumber>,
}

pub fn switch_is_active(switch: Option<BlockNumber>, block_number: BlockNumber) -> bool {
    block_number >= switch.unwrap_or(BlockNumber(u64::MAX))
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum SealVerificationParams {
    Clique {
        #[serde(deserialize_with = "deserialize_period_as_duration")]
        period: Duration,
        epoch: u64,
    },
    Ethash {
        duration_limit: u64,
        block_reward: BTreeMap<BlockNumber, U256>,
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            with = "::serde_with::rust::unwrap_or_skip"
        )]
        homestead_formula: Option<BlockNumber>,
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            with = "::serde_with::rust::unwrap_or_skip"
        )]
        byzantium_formula: Option<BlockNumber>,
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            with = "::serde_with::rust::unwrap_or_skip"
        )]
        difficulty_bomb: Option<DifficultyBomb>,
        #[serde(default)]
        skip_pow_verification: bool,
    },
}

impl SealVerificationParams {
    pub fn gather_forks(&self) -> BTreeSet<BlockNumber> {
        match self {
            SealVerificationParams::Ethash {
                block_reward,
                homestead_formula,
                byzantium_formula,
                difficulty_bomb,
                ..
            } => block_reward
                .keys()
                .copied()
                .chain(*homestead_formula)
                .chain(*byzantium_formula)
                .chain(
                    difficulty_bomb
                        .as_ref()
                        .map(|v| v.delays.keys().copied().collect::<Vec<_>>())
                        .unwrap_or_default(),
                )
                .collect(),
            _ => BTreeSet::new(),
        }
    }
}

// deserialize_str_as_u64
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
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
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Params {
    pub chain_id: ChainId,
    pub network_id: NetworkId,
    pub min_gas_limit: u64,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq)]
pub enum BlockScore {
    NoTurn = 1,
    InTurn = 2,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum Seal {
    Ethash {
        #[serde(with = "hexbytes")]
        vanity: Bytes,
        difficulty: U256,
        nonce: H64,
        mix_hash: H256,
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
            Seal::Clique { score, .. } => (*score as u8).into(),
        }
    }

    pub fn extra_data(&self) -> Bytes {
        match self {
            Seal::Ethash { vanity, .. } => vanity.clone(),
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

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Genesis {
    pub number: BlockNumber,
    pub author: Address,
    pub gas_limit: u64,
    pub timestamp: u64,
    pub seal: Seal,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum Contract {
    Contract {
        #[serde(with = "hexbytes")]
        code: Bytes,
    },
    Precompile(Precompile),
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum ModExpVersion {
    ModExp198,
    ModExp2565,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
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

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct P2PParams {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub bootnodes: Vec<NodeUrl>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub preverified_hashes: Vec<H256>,
}

struct DeserializePeriodAsDuration;

impl<'de> de::Visitor<'de> for DeserializePeriodAsDuration {
    type Value = Duration;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("an u64")
    }

    fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Duration::from_millis(v))
    }
}

fn deserialize_period_as_duration<'de, D>(deserializer: D) -> Result<Duration, D::Error>
where
    D: de::Deserializer<'de>,
{
    deserializer.deserialize_any(DeserializePeriodAsDuration)
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
                },
                params: Params {
                    chain_id: ChainId(4),
                    network_id: NetworkId(4),
                    min_gas_limit: 5000,
                },
                genesis: Genesis {
                    number: BlockNumber(0),
                    author: hex!("0000000000000000000000000000000000000000").into(),
                    gas_limit: 0x47b760,
                    timestamp: 0x58ee40ba,
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
                    0.into() => hashmap! {
                        hex!("31b98d14007bdee637298086988a0bbd31184523").into() => U256::from_be_bytes(hex!("0200000000000000000000000000000000000000000000000000000000000000")),
                    },
                },
                p2p: P2PParams {
                    bootnodes: vec![
                        "enode://a24ac7c5484ef4ed0c5eb2d36620ba4e4aa13b8c84684e1b4aab0cebea2ae45cb4d375b77eab56516d34bfbd3c1a833fc51296ff084b770b94fb9028c4d25ccf@52.169.42.101:30303",
                        "enode://343149e4feefa15d882d9fe4ac7d88f885bd05ebb735e547f12e12080a9fa07c8014ca6fd7f373123488102fe5e34111f8509cf0b7de3f5b44339c9f25e87cb8@52.3.158.184:30303",
                        "enode://b6b28890b006743680c52e64e0d16db57f28124885595fa03a562be1d2bf0f3a1da297d56b13da25fb992888fd556d4c1a27b1f39d531bde7de1921c90061cc6@159.89.28.211:30303",
                    ].into_iter().map(ToString::to_string).collect(),
                    preverified_hashes: vec![],
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
                9_200_000, 12_244_000, 12_965_000, 13_773_000
            ]
            .into_iter()
            .map(BlockNumber)
            .collect()
        );
    }
}
