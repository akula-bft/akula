use std::{collections::HashMap, time::Duration};

use ethereum_types::{H160, H256, U256, U64};
use serde::{de, Deserialize};

type NodeUrl = String;

#[derive(Debug, Deserialize, PartialEq)]
struct ChainSpec {
    name: String,
    data_dir: String,
    bootnodes: Vec<NodeUrl>,
    engine: Engine,
    hardforks: HardForks,
    params: Params,
    genesis: Genesis,
    precompiles: HashMap<U64, HashMap<H160, Precompiles>>,
    balances: HashMap<H160, U256>,
}

#[derive(Debug, Deserialize, PartialEq)]
struct Engine {
    name: String,
    params: EngineParams,
}

#[derive(Debug, Deserialize, PartialEq)]
struct EngineParams {
    #[serde(deserialize_with = "deserialize_period_as_duration")]
    period: Duration,
    epoch: u64,
    genesis: EngineGenesis,
}

#[derive(Debug, Deserialize, PartialEq)]
struct EngineGenesis {
    vanity: H256,
    signers: Vec<H160>,
}

// deserialize_str_as_u64
#[derive(Debug, Deserialize, PartialEq)]
struct HardForks {
    #[serde(deserialize_with = "deserialize_str_as_u64")]
    eip140: u64,
    #[serde(deserialize_with = "deserialize_str_as_u64")]
    eip145: u64,
    #[serde(deserialize_with = "deserialize_str_as_u64")]
    eip150: u64,
    #[serde(deserialize_with = "deserialize_str_as_u64")]
    eip155: u64,
    #[serde(deserialize_with = "deserialize_str_as_u64")]
    eip160: u64,
    #[serde(deserialize_with = "deserialize_str_as_u64")]
    eip161abc: u64,
    #[serde(deserialize_with = "deserialize_str_as_u64")]
    eip161d: u64,
    #[serde(deserialize_with = "deserialize_str_as_u64")]
    eip211: u64,
    #[serde(deserialize_with = "deserialize_str_as_u64")]
    eip214: u64,
    #[serde(deserialize_with = "deserialize_str_as_u64")]
    eip658: u64,
    #[serde(deserialize_with = "deserialize_str_as_u64")]
    eip1014: u64,
    #[serde(deserialize_with = "deserialize_str_as_u64")]
    eip1052: u64,
    #[serde(deserialize_with = "deserialize_str_as_u64")]
    eip1283: u64,
    #[serde(deserialize_with = "deserialize_str_as_u64")]
    eip1283_disable: u64,
    #[serde(deserialize_with = "deserialize_str_as_u64")]
    eip1283_reenable: u64,
    #[serde(deserialize_with = "deserialize_str_as_u64")]
    eip1344: u64,
    #[serde(deserialize_with = "deserialize_str_as_u64")]
    eip1706: u64,
    #[serde(deserialize_with = "deserialize_str_as_u64")]
    eip1884: u64,
    #[serde(deserialize_with = "deserialize_str_as_u64")]
    eip2028: u64,
    #[serde(deserialize_with = "deserialize_str_as_u64")]
    max_code_size: u64,
}

#[derive(Debug, Deserialize, PartialEq)]
struct Params {
    #[serde(deserialize_with = "deserialize_str_as_u64")]
    account_start_nonce: u64,
    #[serde(deserialize_with = "deserialize_str_as_u64")]
    chain_id: u64,
    #[serde(deserialize_with = "deserialize_str_as_u64")]
    gas_limit_bound_divisor: u64,
    #[serde(deserialize_with = "deserialize_str_as_u64")]
    max_code_size: u64,
    #[serde(deserialize_with = "deserialize_str_as_u64")]
    maximum_extra_data_size: u64,
    #[serde(deserialize_with = "deserialize_str_as_u64")]
    min_gas_limit: u64,
    #[serde(deserialize_with = "deserialize_str_as_u64")]
    network_id: u64,
}

#[derive(Debug, Deserialize, PartialEq)]
struct Genesis {
    #[serde(deserialize_with = "deserialize_str_as_u64")]
    gas_limit: u64,
    #[serde(deserialize_with = "deserialize_str_as_u64")]
    timestamp: u64,
}

#[derive(Debug, Deserialize, PartialEq)]
struct Precompiles {
    name: String,
    pricing: Pricing,
}

#[derive(Debug, Deserialize, PartialEq)]
struct Pricing {
    formula: String,
    params: PricingParams,
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(untagged)]
enum PricingParams {
    ModExp { divisor: u64 },
    Linear { base: u64, word: u64 },
    Price { price: u64 },
    BasePair { base: u64, pair: u64 },
    GasPerRound { gas_per_round: u64 },
}

struct DeserializePeriodAsDuration;

impl<'de> de::Visitor<'de> for DeserializePeriodAsDuration {
    type Value = Duration;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("an u64")
    }

    fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Duration::from_millis(v as u64))
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
    use hex_literal::hex;

    #[test]
    fn load_chainspec_struct() {
        let chain_spec = toml::from_str::<ChainSpec>(include_str!("chains/rinkeby.toml")).unwrap();
        let precompiles_0 = vec![(
            hex!("0000000000000000000000000000000000000001").into(),
            Precompiles {
                name: "ecrecover".into(),
                pricing: Pricing {
                    formula: "linear".into(),
                    params: PricingParams::Linear {
                        base: 3000,
                        word: 0,
                    },
                },
            },
        )]
        .into_iter()
        .collect();

        let precompiles_0xfcc25 = vec![
            (
                hex!("0000000000000000000000000000000000000005").into(),
                Precompiles {
                    name: "modexp".into(),
                    pricing: Pricing {
                        formula: "modexp".into(),
                        params: PricingParams::ModExp { divisor: 20 },
                    },
                },
            ),
            (
                hex!("0000000000000000000000000000000000000006").into(),
                Precompiles {
                    name: "alt_bn128_add".into(),
                    pricing: Pricing {
                        formula: "alt_bn128_const_operations".into(),
                        params: PricingParams::Price { price: 500 },
                    },
                },
            ),
            (
                hex!("0000000000000000000000000000000000000008").into(),
                Precompiles {
                    name: "alt_bn128_pairing".into(),
                    pricing: Pricing {
                        formula: "alt_bn128_pairing".into(),
                        params: PricingParams::BasePair {
                            base: 100000,
                            pair: 80000,
                        },
                    },
                },
            ),
        ]
        .into_iter()
        .collect();

        let precompiles_0x52efd1 = vec![
            (
                hex!("0000000000000000000000000000000000000008").into(),
                Precompiles {
                    name: "alt_bn128_pairing".into(),
                    pricing: Pricing {
                        formula: "alt_bn128_pairing".into(),
                        params: PricingParams::BasePair {
                            base: 45_000,
                            pair: 34_000,
                        },
                    },
                },
            ),
            (
                hex!("0000000000000000000000000000000000000009").into(),
                Precompiles {
                    name: "blake2_f".into(),
                    pricing: Pricing {
                        formula: "blake2_f".into(),
                        params: PricingParams::GasPerRound { gas_per_round: 1 },
                    },
                },
            ),
        ]
        .into_iter()
        .collect();

        assert_eq!(
            ChainSpec {
                name: "Rinkeby".into(),
                data_dir: "rinkeby".into(),
                bootnodes: vec![
                    "enode://a24ac7c5484ef4ed0c5eb2d36620ba4e4aa13b8c84684e1b4aab0cebea2ae45cb4d375b77eab56516d34bfbd3c1a833fc51296ff084b770b94fb9028c4d25ccf@52.169.42.101:30303".into(),
                    "enode://343149e4feefa15d882d9fe4ac7d88f885bd05ebb735e547f12e12080a9fa07c8014ca6fd7f373123488102fe5e34111f8509cf0b7de3f5b44339c9f25e87cb8@52.3.158.184:30303".into(),
                    "enode://b6b28890b006743680c52e64e0d16db57f28124885595fa03a562be1d2bf0f3a1da297d56b13da25fb992888fd556d4c1a27b1f39d531bde7de1921c90061cc6@159.89.28.211:30303".into(),
                ],
                engine: Engine {
                    name: "clique".into(),
                    params: EngineParams{
                        period: Duration::from_millis(15),
                        epoch: 30_000,
                        genesis: EngineGenesis {
                            vanity: hex!("52657370656374206d7920617574686f7269746168207e452e436172746d616e").into() , 
                        signers:
                            vec![
                                hex!("42eb768f2244c8811c63729a21a3569731535f06").into(),
                                hex!("7ffc57839b00206d1ad20c69a1981b489f772031").into(),
                                hex!("b279182d99e65703f0076e4812653aab85fca0f0").into(),
                            ],
                        }
                    }
                },
                hardforks: HardForks {
                    eip140: 0xfcc25,
                    eip145: 0x37db77,
                    eip150: 0x2, eip155: 0x3, eip160: 0x0, eip161abc: 0x0, eip161d: 0x0, eip211: 0xfcc25, eip214: 0xfcc25, eip658: 0xfcc25, eip1014: 0x37db77, eip1052: 0x37db77, eip1283: 0x37db77, eip1283_disable: 0x41efd2, eip1283_reenable: 0x52efd1, eip1344: 0x52efd1, eip1706: 0x52efd1, eip1884: 0x52efd1, eip2028: 0x52efd1, max_code_size: 0x0 },
                    params: Params { account_start_nonce: 0x0, chain_id: 0x4, gas_limit_bound_divisor: 0x400, max_code_size: 0x6000, maximum_extra_data_size: 0xffff, min_gas_limit: 0x1388, network_id: 0x4 },
                    genesis: Genesis { gas_limit: 0x47b760, timestamp: 0x58ee40ba
                },
                precompiles: vec![(0, precompiles_0), (0xfcc25, precompiles_0xfcc25), (0x52efd1, precompiles_0x52efd1)].into_iter().map(|(b, set)| (b.into(), set)).collect(),
                balances: vec![
                    (hex!("0000000000000000000000000000000000000000").into(), "0x1".into()),
                    (hex!("31b98d14007bdee637298086988a0bbd31184523").into(), "0x200000000000000000000000000000000000000000000000000000000000000".into())
                    ].into_iter().collect(),
            },
            chain_spec,
        );
    }
}
