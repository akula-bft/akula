use crate::models::ChainSpec;
use once_cell::sync::Lazy;

pub static MAINNET: Lazy<ChainSpec> =
    Lazy::new(|| ron::from_str(include_str!("ethereum.ron")).unwrap());
pub static ROPSTEN: Lazy<ChainSpec> =
    Lazy::new(|| ron::from_str(include_str!("ropsten.ron")).unwrap());
pub static RINKEBY: Lazy<ChainSpec> =
    Lazy::new(|| ron::from_str(include_str!("rinkeby.ron")).unwrap());
pub static SEPOLIA: Lazy<ChainSpec> =
    Lazy::new(|| ron::from_str(include_str!("sepolia.ron")).unwrap());
pub static GOERLI: Lazy<ChainSpec> =
    Lazy::new(|| ron::from_str(include_str!("goerli.ron")).unwrap());

#[cfg(test)]
mod tests {}
