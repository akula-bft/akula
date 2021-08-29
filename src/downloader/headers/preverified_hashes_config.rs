use std::collections::HashMap;

/// The preverified hashes is a list of known precomputed hashes of every 192-th block in the chain:
///
/// hash(0), hash(192), hash(384), hash(576), ...
///
/// The preverified hashes are copied from:
/// https://github.com/ledgerwatch/erigon/blob/devel/turbo/stages/headerdownload/preverified_hashes_mainnet.go
/// https://github.com/ledgerwatch/erigon/blob/devel/turbo/stages/headerdownload/preverified_hashes_ropsten.go
pub struct PreverifiedHashesConfig {
    pub hashes: Vec<ethereum_types::H256>,
}

impl PreverifiedHashesConfig {
    pub fn new(chain_name: &str) -> anyhow::Result<Self> {
        let config_text = match chain_name {
            "mainnet" => include_str!("preferified_hashes_mainnet.toml"),
            "ropsten" => include_str!("preferified_hashes_ropsten.toml"),
            _ => anyhow::bail!("unsupported chain"),
        };
        let config: HashMap<String, Vec<String>> = toml::from_str(config_text)?;
        Ok(PreverifiedHashesConfig {
            hashes: PreverifiedHashesConfig::parse_hashes(&config["hashes"]),
        })
    }

    fn parse_hashes(hash_strings: &[String]) -> Vec<ethereum_types::H256> {
        hash_strings
            .iter()
            .map(|hash_str| {
                let mut hash_bytes = [0u8; 32];
                hex::decode_to_slice(hash_str, &mut hash_bytes).unwrap();
                ethereum_types::H256::from(hash_bytes)
            })
            .collect()
    }
}
