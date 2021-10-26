use once_cell::sync::Lazy;

use crate::genesis::GenesisData;

pub static MAINNET: Lazy<GenesisData> =
    Lazy::new(|| serde_json::from_str(include_str!("mainnet.json")).unwrap());
