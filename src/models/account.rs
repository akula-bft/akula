use anyhow::bail;
use ethereum_types::{H256, U256};
use modular_bitfield::prelude::*;
use static_bytes::Buf;

use crate::common;

#[derive(Clone, Debug, PartialEq)]
pub struct Account {
    pub initialised: bool,
    pub nonce: u64,
    pub balance: U256,
    pub root: Option<H256>,      // merkle root of the storage trie
    pub code_hash: Option<H256>, // hash of the bytecode
    pub incarnation: u64,
}

impl Default for Account {
    fn default() -> Self {
        Self {
            initialised: true,
            nonce: 0,
            balance: U256::zero(),
            root: None,
            code_hash: None,
            incarnation: 0,
        }
    }
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
    dummy: B4,
}

impl Account {
    pub fn encoding_length_for_storage(&self) -> usize {
        let mut struct_length = 1; // 1 byte for fieldset

        if !self.balance.is_zero() {
            struct_length += 1 + Self::u256_compact_len(self.balance);
        }

        if self.nonce > 0 {
            struct_length += 1 + Self::u64_compact_len(self.nonce);
        }

        if self.code_hash.is_some() {
            struct_length += 33 // 32-byte array + 1 byte for length
        }

        if self.incarnation > 0 {
            struct_length += 1 + Self::u64_compact_len(self.incarnation);
        }

        struct_length
    }

    fn u256_compact_len(num: U256) -> usize {
        (num.bits() + 7) / 8
    }

    fn u64_compact_len(num: u64) -> usize {
        ((u64::BITS - num.leading_zeros()) as usize + 7) / 8
    }

    fn write_compact(input: &[u8], buffer: &mut [u8]) -> usize {
        let mut written = 0;
        for &byte in input.iter().skip_while(|v| **v == 0) {
            written += 1;
            buffer[written] = byte;
        }
        if written > 0 {
            buffer[0] = written as u8;
        }

        written
    }

    pub fn encode_for_storage(&self) -> Vec<u8> {
        let mut buffer = vec![0; self.encoding_length_for_storage()];

        let mut field_set = AccountStorageFlags::default(); // start with first bit set to 0
        let mut pos = 1;
        if self.nonce > 0 {
            field_set.set_nonce(true);
            pos += 1 + Self::write_compact(&self.nonce.to_be_bytes(), &mut buffer[pos..]);
        }

        // Encoding balance
        if !self.balance.is_zero() {
            field_set.set_balance(true);
            pos +=
                1 + Self::write_compact(&common::value_to_bytes(self.balance), &mut buffer[pos..]);
        }

        if self.incarnation > 0 {
            field_set.set_incarnation(true);
            pos += 1 + Self::write_compact(&self.incarnation.to_be_bytes(), &mut buffer[pos..]);
        }

        // Encoding code hash
        if let Some(code_hash) = self.code_hash {
            field_set.set_code_hash(true);
            buffer[pos] = 32;
            buffer[pos + 1..pos + 33].copy_from_slice(code_hash.as_bytes());
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

        let field_set_flag = enc.get_u8();
        let field_set = AccountStorageFlags::from_bytes(field_set_flag.to_be_bytes());

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

            a.incarnation = bytes_to_u64(&enc[..decode_length]);
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

            a.code_hash = Some(H256::from_slice(&enc[..decode_length]));
            enc.advance(decode_length);
        }

        Ok(Some(a))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common;
    use hex_literal::hex;

    fn run_test_storage(original: Account, expected_encoded: &[u8]) {
        let encoded_account = original.encode_for_storage();

        assert_eq!(encoded_account, expected_encoded);

        let mut decoded = Account::decode_for_storage(&encoded_account)
            .unwrap()
            .unwrap();

        decoded.root = original.root;

        assert_eq!(original, decoded);
    }

    #[test]
    fn empty() {
        run_test_storage(
            Account {
                initialised: true,
                nonce: 100,
                balance: U256::zero(),
                root: None,
                code_hash: None,
                incarnation: 5,
            },
            &hex!("0501640105"),
        )
    }

    #[test]
    fn with_code() {
        run_test_storage(
            Account {
                initialised: true,
                nonce: 2,
                balance: 1000.into(),
                root: Some(H256(hex!(
                    "0000000000000000000000000000000000000000000000000000000000000021"
                ))),
                code_hash: Some(common::hash_data(&[1, 2, 3])),
                incarnation: 4,
            },
            &hex!("0f01020203e8010420f1885eda54b7a053318cd41e2093220dab15d65381b1157a3633a83bfd5c9239"),
        )
    }

    #[test]
    fn with_code_with_storage_size_hack() {
        run_test_storage(Account {
            initialised: true,
            nonce: 2,
            balance: 1000.into(),
            root: Some(H256(hex!(
                "0000000000000000000000000000000000000000000000000000000000000021"
            ))),
            code_hash: Some(common::hash_data(&[1, 2, 3])),
            incarnation: 5,
        }, &hex!("0f01020203e8010520f1885eda54b7a053318cd41e2093220dab15d65381b1157a3633a83bfd5c9239"))
    }

    #[test]
    fn without_code() {
        run_test_storage(
            Account {
                initialised: true,
                nonce: 2,
                balance: 1000.into(),
                root: None,
                code_hash: None,
                incarnation: 5,
            },
            &hex!("0701020203e80105"),
        )
    }

    #[test]
    fn with_empty_balance_non_nil_contract_and_not_zero_incarnation() {
        run_test_storage(
            Account {
                initialised: true,
                nonce: 0,
                balance: 0.into(),
                root: Some(H256(hex!(
                    "0000000000000000000000000000000000000000000000000000000000000123"
                ))),
                code_hash: Some(H256(hex!(
                    "0000000000000000000000000000000000000000000000000000000000000123"
                ))),
                incarnation: 1,
            },
            &hex!("0c0101200000000000000000000000000000000000000000000000000000000000000123"),
        )
    }

    #[test]
    fn with_empty_balance_and_not_zero_incarnation() {
        run_test_storage(
            Account {
                initialised: true,
                nonce: 0,
                balance: 0.into(),
                root: None,
                code_hash: None,
                incarnation: 1,
            },
            &hex!("040101"),
        )
    }
}
