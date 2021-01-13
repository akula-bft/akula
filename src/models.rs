use anyhow::bail;
use ethereum::Header;
use ethereum_types::{H256, U256};
use hex_literal::hex;
use rlp_derive::RlpDecodable;
use serde::Deserialize;
use std::collections::BTreeSet;

use crate::common;

#[allow(non_snake_case)]
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChainConfig {
    chain_id: Option<u64>,
    homestead_block: Option<u64>,
    dao_fork_block: Option<u64>,
    dao_fork_support: bool,
    eip_150_block: Option<u64>,
    eip_150_hash: Option<H256>,
    eip_155_block: Option<u64>,
    eip_158_block: Option<u64>,
    byzantium_block: Option<u64>,
    constantinople_block: Option<u64>,
    petersburg_block: Option<u64>,
    istanbul_block: Option<u64>,
    muir_glacier_block: Option<u64>,
    yoloV2_block: Option<u64>,
    ewasm_block: Option<u64>,
}

impl ChainConfig {
    pub fn gather_forks(&self) -> BTreeSet<u64> {
        [
            self.homestead_block,
            self.dao_fork_block,
            self.eip_150_block,
            self.eip_155_block,
            self.eip_158_block,
            self.byzantium_block,
            self.constantinople_block,
            self.petersburg_block,
            self.istanbul_block,
            self.muir_glacier_block,
            self.yoloV2_block,
            self.ewasm_block,
        ]
        .iter()
        .filter_map(|b| {
            if let Some(b) = *b {
                if b > 0 {
                    return Some(b);
                }
            }

            None
        })
        .collect()
    }
}

#[derive(RlpDecodable)]
pub struct BodyForStorage {
    pub base_tx_id: u64,
    pub tx_amount: u32,
    pub uncles: Vec<Header>,
}

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
    pub fn decode_for_storage(enc: &[u8]) -> anyhow::Result<Option<Self>> {
        if enc.is_empty() {
            return Ok(None);
        }

        let mut a = Self::default();

        let fieldSet = enc[0];
        let mut pos = 1;

        if fieldSet & 1 > 0 {
            let decodeLength = enc[pos] as usize;

            if enc.len() < pos + decodeLength + 1 {
                bail!(
                    "malformed CBOR for Account.Nonce: 0x{}, Length {}",
                    hex::encode(&enc[pos + 1..]),
                    decodeLength
                );
            }

            a.nonce = bytes_to_u64(&enc[pos + 1..pos + decodeLength + 1]);
            pos += decodeLength + 1;
        }

        if fieldSet & 2 > 0 {
            let decodeLength = enc[pos] as usize;

            if enc.len() < pos + decodeLength + 1 {
                bail!(
                    "malformed CBOR for Account.Nonce: 0x{}, Length {}",
                    hex::encode(&enc[pos + 1..]),
                    decodeLength
                );
            }

            a.balance = U256::from_big_endian(&enc[pos + 1..pos + decodeLength + 1]);
            pos += decodeLength + 1;
        }

        if fieldSet & 4 > 0 {
            let decodeLength = enc[pos] as usize;

            if enc.len() < pos + decodeLength + 1 {
                bail!(
                    "malformed CBOR for Account.Incarnation: 0x{}, Length {}",
                    hex::encode(&enc[pos + 1..]),
                    decodeLength
                )
            }

            a.incarnation = bytes_to_u64(&enc[pos + 1..pos + decodeLength + 1]);
            pos += decodeLength + 1;
        }

        if fieldSet & 8 > 0 {
            let decodeLength = enc[pos] as usize;

            if decodeLength != 32 {
                bail!(
                    "codehash should be 32 bytes long, got {} instead",
                    decodeLength
                )
            }

            if enc.len() < pos + decodeLength + 1 {
                bail!(
                    "malformed CBOR for Account.CodeHash: 0x{}, Length {}",
                    hex::encode(&enc[pos + 1..]),
                    decodeLength
                );
            }

            a.code_hash = H256::from_slice(&enc[pos + 1..pos + decodeLength + 1]);
            pos += decodeLength + 1;
        }

        Ok(Some(a))
    }
}
