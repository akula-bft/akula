use crate::{crypto::*, models::*, u256_to_h256};
use rlp_derive::*;

pub fn create_address(caller: Address, nonce: u64) -> Address {
    #[derive(RlpEncodable)]
    struct V {
        caller: Address,
        nonce: u64,
    }

    Address::from_slice(&keccak256(rlp::encode(&V { caller, nonce })).0[12..])
}

pub fn create2_address(caller: Address, salt: U256, code_hash: H256) -> Address {
    let mut buf = [0_u8; 1 + ADDRESS_LENGTH + KECCAK_LENGTH + KECCAK_LENGTH];
    buf[0] = 0xff;
    buf[1..1 + ADDRESS_LENGTH].copy_from_slice(&caller.0);
    buf[1 + ADDRESS_LENGTH..1 + ADDRESS_LENGTH + KECCAK_LENGTH]
        .copy_from_slice(&u256_to_h256(salt).0);
    buf[1 + ADDRESS_LENGTH + KECCAK_LENGTH..].copy_from_slice(&code_hash.0);

    Address::from_slice(&keccak256(&buf).0[12..])
}

#[cfg(test)]
mod tests {
    use super::*;
    use hex_literal::hex;

    #[test]
    fn create() {
        assert_eq!(
            create_address(hex!("fbe0afcd7658ba86be41922059dd879c192d4c73").into(), 0),
            hex!("c669eaad75042be84daaf9b461b0e868b9ac1871").into()
        );
    }
}
