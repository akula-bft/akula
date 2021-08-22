use crate::common::*;
use ethereum_types::*;
use rlp_derive::*;

pub fn create_address(caller: Address, nonce: u64) -> Address {
    #[derive(RlpEncodable)]
    struct V {
        caller: Address,
        nonce: u64,
    }

    Address::from_slice(&hash_data(rlp::encode(&V { caller, nonce }))[12..])
}

pub fn create2_address(caller: Address, salt: H256, code_hash: H256) -> Address {
    let mut buf = [0_u8; 1 + ADDRESS_LENGTH + HASH_LENGTH + HASH_LENGTH];
    buf[0] = 0xff;
    buf[1..1 + ADDRESS_LENGTH].copy_from_slice(&caller.0);
    buf[1 + ADDRESS_LENGTH..1 + ADDRESS_LENGTH + HASH_LENGTH].copy_from_slice(&salt.0);
    buf[1 + ADDRESS_LENGTH + HASH_LENGTH..].copy_from_slice(&code_hash.0);

    Address::from_slice(&hash_data(buf)[12..])
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
