use crate::models::ChainSpec;
use once_cell::sync::Lazy;

pub static MAINNET: Lazy<ChainSpec> =
    Lazy::new(|| ron::from_str(include_str!("ethereum.ron")).unwrap());
pub static ROPSTEN: Lazy<ChainSpec> =
    Lazy::new(|| ron::from_str(include_str!("ropsten.ron")).unwrap());
pub static RINKEBY: Lazy<ChainSpec> =
    Lazy::new(|| ron::from_str(include_str!("rinkeby.ron")).unwrap());

#[cfg(test)]
mod tests {}
