use crate::models::*;
use serde::{de, Deserialize};
use std::str::FromStr;

/// The preverified hashes is a list of known precomputed hashes of every 192-th block in the chain:
///
/// hash(0), hash(192), hash(384), hash(576), ...
///
/// The preverified hashes are copied from:
/// https://github.com/ledgerwatch/erigon/blob/devel/turbo/stages/headerdownload/preverified_hashes_mainnet.go
/// https://github.com/ledgerwatch/erigon/blob/devel/turbo/stages/headerdownload/preverified_hashes_ropsten.go
#[derive(Clone, Debug)]
pub struct PreverifiedHashesConfig {
    pub hashes: Vec<H256>,
}

struct UnprefixedHexH256(pub H256);

#[derive(Deserialize)]
struct PreverifiedHashesConfigUnprefixedHex {
    pub hashes: Vec<UnprefixedHexH256>,
}

impl PreverifiedHashesConfig {
    pub fn new(chain_name: &str) -> anyhow::Result<Self> {
        let config_text = match chain_name.to_lowercase().as_str() {
            "mainnet" | "ethereum" => include_str!("preverified_hashes_mainnet.toml"),
            "ropsten" => include_str!("preverified_hashes_ropsten.toml"),
            _ => anyhow::bail!("unsupported chain"),
        };
        let config: PreverifiedHashesConfigUnprefixedHex = toml::from_str(config_text)?;
        Ok(Self {
            hashes: config.hashes.iter().map(|hash| hash.0).collect(),
        })
    }

    pub fn empty() -> Self {
        Self { hashes: Vec::new() }
    }

    pub fn is_empty(&self) -> bool {
        self.hashes.is_empty()
    }
}

impl FromStr for UnprefixedHexH256 {
    type Err = hex::FromHexError;

    fn from_str(hash_str: &str) -> Result<Self, Self::Err> {
        let mut hash_bytes = [0u8; 32];
        hex::decode_to_slice(hash_str, &mut hash_bytes)?;
        let hash = H256::from(hash_bytes);

        Ok(UnprefixedHexH256(hash))
    }
}

impl<'de> Deserialize<'de> for UnprefixedHexH256 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        let hash_str = String::deserialize(deserializer)?;
        FromStr::from_str(&hash_str).map_err(de::Error::custom)
    }
}
