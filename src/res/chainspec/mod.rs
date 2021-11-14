use once_cell::sync::Lazy;
use crate::models::ChainSpec;

pub static MAINNET: Lazy<ChainSpec> =
    Lazy::new(|| ron::from_str(include_str!("ethereum.ron")).unwrap());
pub static RINKEBY: Lazy<ChainSpec> =
    Lazy::new(|| ron::from_str(include_str!("rinkeby.ron")).unwrap());


#[cfg(test)]
mod tests {
}