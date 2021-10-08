use bytes::Bytes;
use ethereum_types::{Address, H256};
use hash256_std_hasher::Hash256StdHasher;
use hash_db::Hasher;
use hex_literal::hex;
use secp256k1::{PublicKey, SECP256K1};
use sha3::{Digest, Keccak256};

pub mod blake2;

/// Concrete `Hasher` impl for the Keccak-256 hash
#[derive(Default, Debug, Clone, PartialEq)]
pub struct KeccakHasher;
impl Hasher for KeccakHasher {
    type Out = H256;

    type StdHasher = Hash256StdHasher;

    const LENGTH: usize = 32;

    fn hash(x: &[u8]) -> Self::Out {
        keccak256(x)
    }
}

/// Generates a trie root hash for a vector of key-value tuples
pub fn trie_root<I, K, V>(input: I) -> H256
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<[u8]> + Ord,
    V: AsRef<[u8]>,
{
    triehash::trie_root::<KeccakHasher, _, _, _>(input)
}

/// Generates a trie root hash for a vector of values
pub fn ordered_trie_root<I, V>(input: I) -> H256
where
    I: IntoIterator<Item = V>,
    V: AsRef<[u8]>,
{
    triehash::ordered_trie_root::<KeccakHasher, I>(input)
}

pub trait TrieEncode {
    fn trie_encode(&self) -> Bytes;
}

pub fn root_hash<T: TrieEncode>(values: &[T]) -> H256 {
    ordered_trie_root(values.iter().map(TrieEncode::trie_encode))
}

pub fn is_valid_signature(r: H256, s: H256, homestead: bool) -> bool {
    const UPPER: H256 = H256(hex!(
        "fffffffffffffffffffffffffffffffebaaedce6af48a03bbfd25e8cd0364141"
    ));

    const HALF_N: H256 = H256(hex!(
        "7fffffffffffffffffffffffffffffff5d576e7357a4501ddfe92f46681b20a0"
    ));

    if r.is_zero() || s.is_zero() {
        return false;
    }

    if r >= UPPER && s >= UPPER {
        return false;
    }
    // https://eips.ethereum.org/EIPS/eip-2
    if homestead && s > HALF_N {
        return false;
    }

    true
}

pub fn generate_key() -> secp256k1::SecretKey {
    secp256k1::SecretKey::new(&mut secp256k1::rand::thread_rng())
}

pub fn to_pubkey(seckey: &secp256k1::SecretKey) -> PublicKey {
    secp256k1::PublicKey::from_secret_key(SECP256K1, seckey)
}

pub fn pubkey_to_address(pubkey: &secp256k1::PublicKey) -> Address {
    Address::from_slice(&keccak256(&pubkey.serialize_uncompressed()[1..]).0[12..])
}

pub fn keccak256(data: impl AsRef<[u8]>) -> H256 {
    H256::from_slice(&Keccak256::digest(data.as_ref()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use hex_literal::hex;
    use secp256k1::SECP256K1;

    #[test]
    fn generate_address() {
        assert_eq!(
            pubkey_to_address(&secp256k1::PublicKey::from_secret_key(
                SECP256K1,
                &secp256k1::SecretKey::from_slice(&hex!(
                    "17bc08619f3b717b022728e84f5f39c3f2b3e2ad00cfecbb689e4c1f7965da5f"
                ))
                .unwrap()
            )),
            hex!("5D6C3f4c505385f4F99057C06F0e265FFc16E829").into()
        );
    }
}
