// Copyright 2015-2020 Parity Technologies (UK) Ltd.
// This file is part of OpenEthereum.

// OpenEthereum is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// OpenEthereum is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with OpenEthereum.  If not, see <http://www.gnu.org/licenses/>.

//! Utils implement

use crate::consensus::parlia::*;
use ethereum_types::{H64, H256, H520};
use lazy_static::lazy_static;
use rustc_hex::ToHex;
use std::{
    collections::{BTreeSet, HashSet},
    str::FromStr,
};
use bytes::Buf;
use ethereum_types::Address;
use crate::crypto;
use sha3::{Digest, Keccak256};
use ethereum::*;
/// How many recovered signature to cache in the memory.
const CREATOR_CACHE_NUM: usize = 4096;
use secp256k1::{
    ecdsa::{RecoverableSignature, RecoveryId},
    Message as SecpMessage, SECP256K1,
};

lazy_static! {
    /// key: header hash
    /// value: creator address
    static ref CREATOR_BY_HASH: RwLock<LruCache<H256, Address>> = RwLock::new(LruCache::new(CREATOR_CACHE_NUM));

    pub static ref SYSTEM_ACCOUNT: Address = Address::from_str("ffffFFFfFFffffffffffffffFfFFFfffFFFfFFfE").unwrap();
    pub static ref VALIDATOR_CONTRACT: Address =  Address::from_str("0000000000000000000000000000000000001000").unwrap();
    pub static ref SLASH_CONTRACT: Address =  Address::from_str("0000000000000000000000000000000000001001").unwrap();
    pub static ref SYSTEM_REWARD_CONTRACT: Address = Address::from_str("0000000000000000000000000000000000001002").unwrap();
    pub static ref SYSTEM_CONTRACTS: HashSet<Address> = [
        "0000000000000000000000000000000000001000",
        "0000000000000000000000000000000000001001",
        "0000000000000000000000000000000000001002",
        "0000000000000000000000000000000000001003",
        "0000000000000000000000000000000000001004",
        "0000000000000000000000000000000000001005",
        "0000000000000000000000000000000000001006",
        "0000000000000000000000000000000000001007",
        "0000000000000000000000000000000000001008",
        "0000000000000000000000000000000000002000",
    ]
    .iter()
    .map(|x| Address::from_str(x).unwrap())
    .collect();
}
pub struct Signature([u8; 65]);

pub fn public_to_address(public: &Public) -> Address {
    let hash = crypto::keccak256(public);
    let mut result = Address::from_slice(&hash[12..]);
    result
}

/// whether the contract is system or not
pub fn is_to_system_contract(addr: &Address) -> bool {
    SYSTEM_CONTRACTS.contains(addr)
}

/// whether the transaction is system or not
pub fn is_system_transaction(tx: &Message, sender: &Address, author: &Address) -> bool {
    if let TransactionAction::Call(to) = tx.action() {
        sender.eq(&author) && is_to_system_contract(&to) && tx.max_fee_per_gas() == 0
    } else {
        false
    }
}

/// Recover block creator from signature
pub fn recover_creator(header: &BlockHeader, chain_id: ChainId) -> Result<Address, DuoError> {
    // Initialization
    let mut cache = CREATOR_BY_HASH.write();

    if let Some(creator) = cache.get_mut(&header.hash()) {
        return Ok(*creator);
    }

    let data = &header.extra_data;

    if data.len() < VANITY_LENGTH + SIGNATURE_LENGTH {
        return Err(Validation(ValidationError::WrongHeaderExtraLen {
            expected: VANITY_LENGTH + SIGNATURE_LENGTH,
            got: data.len()
        }));
    }
    let signature_offset = header.extra_data.len() - SIGNATURE_LENGTH;

    let sig = &header.extra_data[signature_offset..signature_offset + 64];
    let rec = RecoveryId::from_i32(header.extra_data[signature_offset + 64] as i32)?;
    let signature = RecoverableSignature::from_compact(sig, rec)?;

    let mut sig_hash_header = header.clone();
    sig_hash_header.extra_data = Bytes::copy_from_slice(&header.extra_data[..signature_offset]);
    let message = &SecpMessage::from_slice(sig_hash_header.hash_with_chain_id(chain_id.0).as_bytes())?;

    let public = &SECP256K1.recover_ecdsa(message, &signature)?;
    let address_slice = &Keccak256::digest(&public.serialize_uncompressed()[1..])[12..];

    let creator = Address::from_slice(address_slice);
    cache.insert(header.hash(), creator.clone());
    Ok(creator)
}

#[cfg(test)]
mod tests {
    use hex_literal::hex;
    use super::*;

    #[test]
    fn test_bsc_creator_recover() {
        let header = &BlockHeader{
            parent_hash: hex!("0d21840abff46b96c84b2ac9e10e4f5cdaeb5693cb665db62a2f3b02d2d57b5b").into(),
            ommers_hash: hex!("1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347").into(),
            beneficiary: hex!("2a7cdd959bfe8d9487b2a43b33565295a698f7e2").into(),
            state_root: hex!("1db428ea79cb2e8cc233ae7f4db7c3567adfcb699af668a9f583fdae98e95588").into(),
            transactions_root: hex!("53a8743b873570daa630948b1858eaf5dc9bb0bca2093a197e507b2466c110a0").into(),
            receipts_root: hex!("fc7c0fda97e67ed8ae06e7a160218b3df995560dfcb209a3b0dddde969ec6b00").into(),
            logs_bloom: hex!("08000000000000000000000000000000000000000000000000000000000000000000000000000000000000000020000000000000100000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000040000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000300000000000000000000000000000").into(),
            difficulty: u256::new(2),
            number: BlockNumber(1),
            gas_limit: 39843751 as u64,
            gas_used: 3148599 as u64,
            timestamp: 1598671449 as u64,
            extra_data: hex::decode("d883010002846765746888676f312e31332e34856c696e757800000000000000924cd67a1565fdd24dd59327a298f1d702d6b7a721440c063713cecb7229f4e162ae38be78f6f71aa5badeaaef35cea25061ee2100622a4a1631a07e862b517401").unwrap().into(),
            mix_hash: hex!("0000000000000000000000000000000000000000000000000000000000000000").into(),
            nonce: hex!("0000000000000000").into(),
            base_fee_per_gas: None
        };
        info!("test header {}:{}", header.number.0, header.hash());
        assert_eq!(header.hash(), hex!("04055304e432294a65ff31069c4d3092ff8b58f009cdb50eba5351e0332ad0f6").into());
        let addr = recover_creator(header, &(56_u64)).unwrap();
        assert_eq!(addr, Address::from_str("2a7cdd959bfe8d9487b2a43b33565295a698f7e2").unwrap());
    }
}