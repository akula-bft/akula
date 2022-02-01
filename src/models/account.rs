use crate::{kv::tables::VariableVec, models::*, util::*};
use arrayvec::ArrayVec;
use bytes::{Buf, Bytes};
use educe::*;
use modular_bitfield::prelude::*;
use rlp_derive::*;
use serde::*;
use std::collections::HashMap;

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Account {
    pub nonce: u64,
    pub balance: U256,
    pub code_hash: H256, // hash of the bytecode
}

#[derive(Debug, RlpEncodable, RlpDecodable)]
pub struct RlpAccount {
    pub nonce: u64,
    pub balance: U256,
    pub storage_root: H256,
    pub code_hash: H256,
}

impl Default for Account {
    fn default() -> Self {
        Self {
            nonce: 0,
            balance: U256::ZERO,
            code_hash: EMPTY_HASH,
        }
    }
}

#[derive(Deserialize, Educe)]
#[educe(Debug)]
pub struct SerializedAccount {
    pub balance: U256,
    #[serde(with = "hexbytes")]
    #[educe(Debug(method = "write_hex_string"))]
    pub code: Bytes,
    pub nonce: U64,
    pub storage: HashMap<U256, U256>,
}

fn bytes_to_u64(buf: &[u8]) -> u64 {
    let mut decoded = [0u8; 8];
    for (i, b) in buf.iter().rev().enumerate() {
        decoded[i] = *b;
    }

    u64::from_le_bytes(decoded)
}

#[allow(dead_code)]
#[bitfield]
#[derive(Clone, Copy, Debug, Default)]
struct AccountStorageFlags {
    nonce_len: B4,
    code_hash: bool,
    #[skip]
    unused: B3,
}

pub const MAX_ACCOUNT_LEN: usize = 1 + (1 + 32) + (1 + 32) + (1 + 8);

pub type EncodedAccount = VariableVec<MAX_ACCOUNT_LEN>;

impl Account {
    fn write_compact<const LEN: usize>(input: &[u8; LEN]) -> ArrayVec<u8, LEN> {
        input.iter().copied().skip_while(|v| *v == 0).collect()
    }

    pub fn encode_for_storage(&self) -> EncodedAccount {
        let mut buffer = EncodedAccount::default();

        let mut field_set = AccountStorageFlags::default(); // start with first bit set to 0
        buffer.push(0);
        if self.nonce != 0 {
            let b = Self::write_compact(&self.nonce.to_be_bytes());
            field_set.set_nonce_len(b.len().try_into().unwrap());
            buffer.try_extend_from_slice(&b[..]).unwrap();
        }

        // Encoding code hash
        if self.code_hash != EMPTY_HASH {
            field_set.set_code_hash(true);
            buffer
                .try_extend_from_slice(self.code_hash.as_fixed_bytes())
                .unwrap();
        }

        // Encoding balance
        if self.balance != 0 {
            let b = Self::write_compact(&self.balance.to_be_bytes());
            buffer.try_extend_from_slice(&b[..]).unwrap();
        }

        let fs = field_set.into_bytes()[0];
        buffer[0] = fs;

        buffer
    }

    pub fn decode_for_storage(mut enc: &[u8]) -> anyhow::Result<Option<Self>> {
        if enc.is_empty() {
            return Ok(None);
        }

        let mut a = Self::default();

        let field_set = AccountStorageFlags::from_bytes([enc.get_u8()]);

        let decode_length = field_set.nonce_len();
        if decode_length > 0 {
            a.nonce = bytes_to_u64(&enc[..decode_length.into()]);
            enc.advance(decode_length.into());
        }

        if field_set.code_hash() {
            a.code_hash = H256::from_slice(&enc[..KECCAK_LENGTH]);
            enc.advance(KECCAK_LENGTH);
        }

        a.balance = U256::from_be_bytes(static_left_pad(enc));

        Ok(Some(a))
    }

    pub fn to_rlp(&self, storage_root: H256) -> RlpAccount {
        RlpAccount {
            nonce: self.nonce,
            balance: self.balance,
            storage_root,
            code_hash: self.code_hash,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crypto::*;
    use hex_literal::hex;

    fn run_test_storage<const EXPECTED_LEN: usize>(
        original: Account,
        expected_encoded: [u8; EXPECTED_LEN],
    ) {
        let encoded_account = original.encode_for_storage();

        assert_eq!(&encoded_account[..], &expected_encoded[..]);

        let decoded = Account::decode_for_storage(&encoded_account)
            .unwrap()
            .unwrap();

        assert_eq!(original, decoded);
    }

    #[test]
    fn empty() {
        run_test_storage(
            Account {
                nonce: 100,
                balance: 0.as_u256(),
                code_hash: EMPTY_HASH,
            },
            hex!("0164"),
        )
    }

    #[test]
    fn with_code() {
        run_test_storage(
            Account {
                nonce: 2,
                balance: 1000.as_u256(),
                code_hash: keccak256(&[1, 2, 3]),
            },
            hex!("1102f1885eda54b7a053318cd41e2093220dab15d65381b1157a3633a83bfd5c923903e8"),
        )
    }

    #[test]
    fn without_code() {
        run_test_storage(
            Account {
                nonce: 2,
                balance: 1000.as_u256(),
                code_hash: EMPTY_HASH,
            },
            hex!("010203e8"),
        )
    }

    #[test]
    fn with_empty_balance_non_nil_contract() {
        run_test_storage(
            Account {
                nonce: 0,
                balance: 0.as_u256(),
                code_hash: H256(hex!(
                    "0000000000000000000000000000000000000000000000000000000000000123"
                )),
            },
            hex!("100000000000000000000000000000000000000000000000000000000000000123"),
        )
    }

    #[test]
    fn with_empty_balance() {
        run_test_storage(
            Account {
                nonce: 0,
                balance: 0.as_u256(),
                code_hash: EMPTY_HASH,
            },
            hex!("00"),
        )
    }
}
