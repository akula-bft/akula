use ethereum_types::H256;
use hash256_std_hasher::Hash256StdHasher;
use hash_db::Hasher;
use hex_literal::hex;
use sha3::{Digest, Keccak256};

pub mod blake2;
pub mod go_rng;

/// Concrete `Hasher` impl for the Keccak-256 hash
#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub struct KeccakHasher;
impl Hasher for KeccakHasher {
    type Out = H256;

    type StdHasher = Hash256StdHasher;

    const LENGTH: usize = 32;

    fn hash(x: &[u8]) -> Self::Out {
        keccak256(x)
    }
}

#[cfg(test)]
/// Generates a trie root hash for a vector of key-value tuples
pub fn trie_root<I, K, V>(input: I) -> H256
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<[u8]> + Ord,
    V: AsRef<[u8]>,
{
    triehash::trie_root::<KeccakHasher, _, _, _>(input)
}

pub fn is_valid_signature(r: H256, s: H256) -> bool {
    const UPPER: H256 = H256(hex!(
        "fffffffffffffffffffffffffffffffebaaedce6af48a03bbfd25e8cd0364141"
    ));

    if r.is_zero() || s.is_zero() {
        return false;
    }

    if r >= UPPER && s >= UPPER {
        return false;
    }

    true
}

pub fn keccak256(data: impl AsRef<[u8]>) -> H256 {
    H256::from_slice(&Keccak256::digest(data.as_ref()))
}
