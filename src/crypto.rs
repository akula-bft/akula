use ethereum_types::Address;
use secp256k1::{PublicKey, SECP256K1};

use crate::common;

pub fn generate_key() -> secp256k1::SecretKey {
    secp256k1::SecretKey::new(&mut secp256k1::rand::thread_rng())
}

pub fn to_pubkey(seckey: &secp256k1::SecretKey) -> PublicKey {
    secp256k1::PublicKey::from_secret_key(SECP256K1, seckey)
}

pub fn pubkey_to_address(pubkey: &secp256k1::PublicKey) -> Address {
    Address::from_slice(&common::hash_data(&pubkey.serialize_uncompressed()[1..]).0[12..])
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
                &SECP256K1,
                &secp256k1::SecretKey::from_slice(&hex!(
                    "17bc08619f3b717b022728e84f5f39c3f2b3e2ad00cfecbb689e4c1f7965da5f"
                ))
                .unwrap()
            )),
            hex!("5D6C3f4c505385f4F99057C06F0e265FFc16E829").into()
        );
    }
}
