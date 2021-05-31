use crate::common;
use anyhow::bail;
use ethereum_types::{H256, U256};
use hex_literal::hex;

#[derive(Clone, Debug, PartialEq)]
pub struct Account {
    pub initialised: bool,
    pub nonce: u64,
    pub balance: U256,
    pub root: H256,      // merkle root of the storage trie
    pub code_hash: H256, // hash of the bytecode
    pub incarnation: u64,
}

impl Default for Account {
    fn default() -> Self {
        Self {
            initialised: true,
            nonce: 0,
            balance: U256::zero(),
            root: hex!("56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421").into(),
            code_hash: common::hash_data(&[]),
            incarnation: 0,
        }
    }
}

fn bytes_to_u64(buf: &[u8]) -> u64 {
    let mut x = 0;
    for (i, b) in buf.iter().enumerate() {
        x <<= 8 + *b as u64;
        if i == 7 {
            break;
        }
    }
    x
}

impl Account {
    pub fn encoding_length_for_storage(&self) -> usize {
        let mut struct_length = 1; // 1 byte for fieldset

        if !self.balance.is_zero() {
            struct_length += (self.balance.bits() + 7) / 8 + 1;
        }

        if self.nonce > 0 {
            struct_length += (u64::BITS - self.nonce.leading_zeros()) as usize;
        }

        if self.is_empty_code_hash() {
            struct_length += 33 // 32-byte array + 1 byte for length
        }

        if self.incarnation > 0 {
            struct_length += (u64::BITS - self.incarnation.leading_zeros()) as usize;
        }

        struct_length
    }

    fn write_compact<
        'a,
        I: IntoIterator<Item = &'a u8, IntoIter = It>,
        It: Iterator<Item = &'a u8>,
    >(
        iter: I,
        buffer: &mut [u8],
    ) -> usize {
        let mut written = 0;
        for &byte in iter.into_iter().skip_while(|v| **v == 0) {
            written += 1;
            buffer[written] = byte;
        }
        if written > 0 {
            buffer[0] = written as u8;
        }

        written
    }

    pub fn encode_for_storage(&self, buffer: &mut [u8]) {
        let mut field_set = 0; // start with first bit set to 0
        let mut pos = 1;
        if self.nonce > 0 {
            field_set = 1;
            pos += Self::write_compact(&self.nonce.to_be_bytes(), &mut buffer[pos..]);
        }

        // Encoding balance
        if !self.balance.is_zero() {
            field_set |= 2;
            pos += Self::write_compact(&<[u8; 32]>::from(self.balance), &mut buffer[pos..]);
        }

        if self.incarnation > 0 {
            field_set |= 4;
            pos += Self::write_compact(&self.incarnation.to_be_bytes(), &mut buffer[pos..]);
        }

        // Encoding CodeHash
        if !self.is_empty_code_hash() {
            field_set |= 8;
            buffer[pos] = 32;
            buffer[pos + 1..pos + 33].copy_from_slice(self.code_hash.as_bytes());
            //pos += 33;
        }

        buffer[0] = field_set;
    }

    pub fn is_empty_code_hash(&self) -> bool {
        self.code_hash == common::hash_data(&[]) || self.code_hash == H256::zero()
    }

    pub fn decode_for_storage(enc: &[u8]) -> anyhow::Result<Option<Self>> {
        if enc.is_empty() {
            return Ok(None);
        }

        let mut a = Self::default();

        let field_set = enc[0];
        let mut pos = 1;

        if field_set & 1 > 0 {
            let decode_length = enc[pos] as usize;

            if enc.len() < pos + decode_length + 1 {
                bail!(
                    "malformed CBOR for Account.Nonce: 0x{}, Length {}",
                    hex::encode(&enc[pos + 1..]),
                    decode_length
                );
            }

            a.nonce = bytes_to_u64(&enc[pos + 1..pos + decode_length + 1]);
            pos += decode_length + 1;
        }

        if field_set & 2 > 0 {
            let decode_length = enc[pos] as usize;

            if enc.len() < pos + decode_length + 1 {
                bail!(
                    "malformed CBOR for Account.Nonce: 0x{}, Length {}",
                    hex::encode(&enc[pos + 1..]),
                    decode_length
                );
            }

            a.balance = U256::from_big_endian(&enc[pos + 1..pos + decode_length + 1]);
            pos += decode_length + 1;
        }

        if field_set & 4 > 0 {
            let decode_length = enc[pos] as usize;

            if enc.len() < pos + decode_length + 1 {
                bail!(
                    "malformed CBOR for Account.Incarnation: 0x{}, Length {}",
                    hex::encode(&enc[pos + 1..]),
                    decode_length
                )
            }

            a.incarnation = bytes_to_u64(&enc[pos + 1..pos + decode_length + 1]);
            pos += decode_length + 1;
        }

        if field_set & 8 > 0 {
            let decode_length = enc[pos] as usize;

            if decode_length != 32 {
                bail!(
                    "codehash should be 32 bytes long, got {} instead",
                    decode_length
                )
            }

            if enc.len() < pos + decode_length + 1 {
                bail!(
                    "malformed CBOR for Account.CodeHash: 0x{}, Length {}",
                    hex::encode(&enc[pos + 1..]),
                    decode_length
                );
            }

            a.code_hash = H256::from_slice(&enc[pos + 1..pos + decode_length + 1]);
            pos += decode_length + 1;
        }

        _ = pos;

        Ok(Some(a))
    }
}
