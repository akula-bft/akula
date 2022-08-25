use bytes::Bytes;
use ethereum_types::Address;
use primitive_types::H256;
use secp256k1::SecretKey;

fn default_extra_data() -> Bytes {
    // TODO replace by version string once we have versioned releases
    Bytes::from("Akula preview")
}

struct BlockProposerParameters {
    random: H256,
    suggested_ether_base: Address,
    timestamp: u64,
}

#[derive(Debug)]
pub struct MiningConfig {
    pub enabled: bool,
    pub ether_base: Address,
    pub secret_key: SecretKey,
    pub extra_data: Option<Bytes>,
}

impl MiningConfig {
    pub fn get_ether_base(&self) -> Address {
        self.ether_base
    }

    pub fn get_extra_data(&self) -> Bytes {
        match &self.extra_data {
            Some(custom) => custom.clone(),
            None => default_extra_data(),
        }
    }
}
