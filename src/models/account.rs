use crate::{kv::tables::VariableVec, models::*, util::*};
use anyhow::bail;
use arrayvec::ArrayVec;
use bytes::{Buf, Bytes};
use educe::*;
use ethereum_types::*;
use modular_bitfield::prelude::*;
use rlp_derive::*;
use serde::*;
use std::collections::HashMap;

#[derive(Clone, Debug, PartialEq)]
pub struct Account {
    pub nonce: u64,
    pub balance: U256,
    pub code_hash: H256, // hash of the bytecode
    pub incarnation: Incarnation,
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
            balance: U256::zero(),
            code_hash: EMPTY_HASH,
            incarnation: Incarnation(0),
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
    nonce: bool,
    balance: bool,
    incarnation: bool,
    code_hash: bool,
    #[skip]
    unused: B4,
}

pub const MAX_ACCOUNT_LEN: usize = 1 + (1 + 32) + (1 + 8) + (1 + 32) + (1 + 8);

pub type EncodedAccount = VariableVec<MAX_ACCOUNT_LEN>;

impl Account {
    fn write_compact<const LEN: usize>(input: &[u8; LEN]) -> ArrayVec<u8, LEN> {
        input.iter().copied().skip_while(|v| *v == 0).collect()
    }

    pub fn encode_for_storage(&self, omit_code_hash: bool) -> EncodedAccount {
        let mut buffer = EncodedAccount::default();

        let mut field_set = AccountStorageFlags::default(); // start with first bit set to 0
        buffer.push(0);
        if self.nonce > 0 {
            field_set.set_nonce(true);
            let b = Self::write_compact(&self.nonce.to_be_bytes());
            buffer.push(b.len().try_into().unwrap());
            buffer.try_extend_from_slice(&b[..]).unwrap();
        }

        // Encoding balance
        if !self.balance.is_zero() {
            field_set.set_balance(true);
            let b = Self::write_compact(&value_to_bytes(self.balance));
            buffer.push(b.len().try_into().unwrap());
            buffer.try_extend_from_slice(&b[..]).unwrap();
        }

        if self.incarnation.0 > 0 {
            field_set.set_incarnation(true);
            let b = Self::write_compact(&self.incarnation.to_be_bytes());
            buffer.push(b.len().try_into().unwrap());
            buffer.try_extend_from_slice(&b[..]).unwrap();
        }

        // Encoding code hash
        if self.code_hash != EMPTY_HASH && !omit_code_hash {
            field_set.set_code_hash(true);
            buffer.push(32);
            buffer
                .try_extend_from_slice(self.code_hash.as_fixed_bytes())
                .unwrap();
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

        if field_set.nonce() {
            let decode_length = enc.get_u8() as usize;

            a.nonce = bytes_to_u64(&enc[..decode_length]);
            enc.advance(decode_length);
        }

        if field_set.balance() {
            let decode_length = enc.get_u8() as usize;

            a.balance = U256::from_big_endian(&enc[..decode_length]);
            enc.advance(decode_length);
        }

        if field_set.incarnation() {
            let decode_length = enc.get_u8() as usize;

            a.incarnation = bytes_to_u64(&enc[..decode_length]).into();
            enc.advance(decode_length);
        }

        if field_set.code_hash() {
            let decode_length = enc.get_u8() as usize;

            if decode_length != 32 {
                bail!(
                    "codehash should be 32 bytes long, got {} instead",
                    decode_length
                )
            }

            a.code_hash = H256::from_slice(&enc[..decode_length]);
            enc.advance(decode_length);
        }

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
        let encoded_account = original.encode_for_storage(false);

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
                balance: U256::zero(),
                code_hash: EMPTY_HASH,
                incarnation: 5.into(),
            },
            hex!("0501640105"),
        )
    }

    #[test]
    fn with_code() {
        run_test_storage(
            Account {
                nonce: 2,
                balance: 1000.into(),
                code_hash: keccak256(&[1, 2, 3]),
                incarnation: 4.into(),
            },
            hex!("0f01020203e8010420f1885eda54b7a053318cd41e2093220dab15d65381b1157a3633a83bfd5c9239"),
        )
    }

    #[test]
    fn with_code_with_storage_size_hack() {
        run_test_storage(Account {
            nonce: 2,
            balance: 1000.into(),
            code_hash: keccak256(&[1, 2, 3]),
            incarnation: 5.into(),
        }, hex!("0f01020203e8010520f1885eda54b7a053318cd41e2093220dab15d65381b1157a3633a83bfd5c9239"))
    }

    #[test]
    fn without_code() {
        run_test_storage(
            Account {
                nonce: 2,
                balance: 1000.into(),
                code_hash: EMPTY_HASH,
                incarnation: 5.into(),
            },
            hex!("0701020203e80105"),
        )
    }

    #[test]
    fn with_empty_balance_non_nil_contract_and_not_zero_incarnation() {
        run_test_storage(
            Account {
                nonce: 0,
                balance: 0.into(),
                code_hash: H256(hex!(
                    "0000000000000000000000000000000000000000000000000000000000000123"
                )),
                incarnation: 1.into(),
            },
            hex!("0c0101200000000000000000000000000000000000000000000000000000000000000123"),
        )
    }

    #[test]
    fn with_empty_balance_and_not_zero_incarnation() {
        run_test_storage(
            Account {
                nonce: 0,
                balance: 0.into(),
                code_hash: EMPTY_HASH,
                incarnation: 1.into(),
            },
            hex!("040101"),
        )
    }
}
