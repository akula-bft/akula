use derive_more::Display;
use serde::Serialize;

/// EVM revision.
#[derive(Clone, Copy, Debug, Display, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize)]
pub enum Revision {
    /// The Frontier revision.
    /// The one Ethereum launched with.
    Frontier = 0,

    /// [The Homestead revision.](https://eips.ethereum.org/EIPS/eip-606)
    Homestead = 1,

    /// [The Tangerine Whistle revision.](https://eips.ethereum.org/EIPS/eip-608)
    Tangerine = 2,

    /// [The Spurious Dragon revision.](https://eips.ethereum.org/EIPS/eip-607)
    Spurious = 3,

    /// [The Byzantium revision.](https://eips.ethereum.org/EIPS/eip-609)
    Byzantium = 4,

    /// [The Constantinople revision.](https://eips.ethereum.org/EIPS/eip-1013)
    Constantinople = 5,

    /// [The Petersburg revision.](https://eips.ethereum.org/EIPS/eip-1716)
    Petersburg = 6,

    /// [The Istanbul revision.](https://eips.ethereum.org/EIPS/eip-1679)
    Istanbul = 7,

    /// [The Berlin revision.](https://github.com/ethereum/eth1.0-specs/blob/master/network-upgrades/mainnet-upgrades/berlin.md)
    Berlin = 8,

    /// [The London revision.](https://github.com/ethereum/eth1.0-specs/blob/master/network-upgrades/mainnet-upgrades/london.md)
    London = 9,

    /// [The Paris revision.](https://github.com/ethereum/eth1.0-specs/blob/master/network-upgrades/mainnet-upgrades/paris.md)
    Paris = 10,
}

impl Revision {
    pub const fn iter() -> [Revision; Revision::len()] {
        [
            Self::Frontier,
            Self::Homestead,
            Self::Tangerine,
            Self::Spurious,
            Self::Byzantium,
            Self::Constantinople,
            Self::Petersburg,
            Self::Istanbul,
            Self::Berlin,
            Self::London,
            Self::Paris,
        ]
    }

    pub const fn latest() -> Self {
        Self::Paris
    }

    pub const fn len() -> usize {
        Self::latest() as usize + 1
    }
}
