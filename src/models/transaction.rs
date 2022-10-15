use super::util::*;
use crate::{
    crypto::{is_valid_signature, keccak256},
    models::*,
    trie::*,
    util::*,
};
use anyhow::{bail, format_err};
use bytes::*;
use derive_more::Deref;
use educe::Educe;
use fastrlp::*;
use hex_literal::hex;
use modular_bitfield::prelude::*;
use num_enum::TryFromPrimitive;
use secp256k1::{
    ecdsa::{RecoverableSignature, RecoveryId},
    Message as SecpMessage, SECP256K1,
};
use sha3::*;
use std::borrow::Cow;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum TxType {
    Legacy = 0,
    EIP2930 = 1,
    EIP1559 = 2,
}

impl TryFrom<u8> for TxType {
    type Error = DecodeError;
    fn try_from(orig: u8) -> Result<Self, Self::Error> {
        match orig {
            0 => Ok(TxType::Legacy),
            1 => Ok(TxType::EIP2930),
            2 => Ok(TxType::EIP1559),
            _ => Err(DecodeError::Custom("Invalid tx type")),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransactionAction {
    Call(Address),
    Create,
}

impl TransactionAction {
    pub fn into_address(self) -> Option<Address> {
        match self {
            TransactionAction::Call(address) => Some(address),
            TransactionAction::Create => None,
        }
    }
}

impl Encodable for TransactionAction {
    fn length(&self) -> usize {
        match self {
            TransactionAction::Call(_) => 1 + ADDRESS_LENGTH,
            TransactionAction::Create => 1,
        }
    }

    fn encode(&self, out: &mut dyn BufMut) {
        match self {
            TransactionAction::Call(address) => {
                Header {
                    list: false,
                    payload_length: Address::len_bytes(),
                }
                .encode(out);
                out.put_slice(address.as_bytes());
            }
            TransactionAction::Create => {
                out.put_u8(EMPTY_STRING_CODE);
            }
        }
    }
}

impl Decodable for TransactionAction {
    fn decode(buf: &mut &[u8]) -> Result<Self, DecodeError> {
        if buf.is_empty() {
            return Err(DecodeError::InputTooShort);
        }

        const ADDRESS_CODE: u8 = EMPTY_STRING_CODE + ADDRESS_LENGTH as u8;

        Ok(match buf.get_u8() {
            EMPTY_STRING_CODE => Self::Create,
            ADDRESS_CODE => {
                let s = buf.get(..20).ok_or(DecodeError::InputTooShort)?;
                buf.advance(20);
                Self::Call(Address::from_slice(s))
            }
            _ => return Err(DecodeError::UnexpectedLength),
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct YParityAndChainId {
    pub odd_y_parity: bool,
    pub chain_id: Option<ChainId>,
}

impl YParityAndChainId {
    pub fn from_v(v: u64) -> Option<Self> {
        if v == 27 || v == 28 {
            // pre EIP-155
            Some(Self {
                odd_y_parity: v == 28,
                chain_id: None,
            })
        } else if v >= 35 {
            // https://eips.ethereum.org/EIPS/eip-155
            // Find chain_id and y_parity ∈ {0, 1} such that
            // v = chain_id * 2 + 35 + y_parity
            let w = v - 35;
            let chain_id = Some(ChainId(w >> 1)); // w / 2
            Some(Self {
                odd_y_parity: (w % 2) != 0,
                chain_id,
            })
        } else {
            None
        }
    }

    pub fn v(&self) -> u64 {
        if let Some(chain_id) = self.chain_id {
            chain_id.0 * 2 + 35 + self.odd_y_parity as u64
        } else {
            27 + self.odd_y_parity as u64
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MessageSignature {
    odd_y_parity: bool,
    r: H256,
    s: H256,
}

impl MessageSignature {
    #[must_use]
    pub fn new(odd_y_parity: bool, r: impl Into<H256>, s: impl Into<H256>) -> Option<Self> {
        let r = r.into();
        let s = s.into();
        if is_valid_signature(r, s) {
            Some(Self { odd_y_parity, r, s })
        } else {
            None
        }
    }

    #[must_use]
    pub fn malleable(&self) -> bool {
        const HALF_N: H256 = H256(hex!(
            "7fffffffffffffffffffffffffffffff5d576e7357a4501ddfe92f46681b20a0"
        ));
        self.s > HALF_N
    }

    #[must_use]
    pub fn odd_y_parity(&self) -> bool {
        self.odd_y_parity
    }

    #[must_use]
    pub fn r(&self) -> &H256 {
        &self.r
    }

    #[must_use]
    pub fn s(&self) -> &H256 {
        &self.s
    }

    #[must_use]
    pub fn is_low_s(&self) -> bool {
        const LOWER: H256 = H256([
            0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
            0xff, 0xff, 0x5d, 0x57, 0x6e, 0x73, 0x57, 0xa4, 0x50, 0x1d, 0xdf, 0xe9, 0x2f, 0x46,
            0x68, 0x1b, 0x20, 0xa0,
        ]);

        self.s <= LOWER
    }
}

#[derive(Clone, Debug, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub struct AccessListItem {
    pub address: Address,
    pub slots: Vec<H256>,
}

pub type AccessList = Vec<AccessListItem>;

#[bitfield]
pub struct MessageVariant {
    version: B2,
    #[skip]
    unused: B6,
}

#[bitfield]
#[derive(Clone, Copy, Debug, Default)]
pub struct LegacyMessageFlags {
    variant: B2,

    v_len: B5,

    is_call: bool,
    nonce_len: B3,
    gas_price_len: B5,
    gas_limit_len: B3,
    value_len: B5,
}

#[bitfield]
#[derive(Clone, Copy, Debug, Default)]
pub struct EIP2930MessageFlags {
    variant: B2,

    v_len: B5,

    is_call: bool,
    nonce_len: B3,
    gas_price_len: B5,
    gas_limit_len: B3,
    value_len: B5,
    access_list_size_len: B3,

    #[skip]
    unused: B5,
}

#[bitfield]
#[derive(Clone, Copy, Debug, Default)]
pub struct EIP1559MessageFlags {
    variant: B2,

    v_len: B5,

    is_call: bool,
    nonce_len: B3,
    max_priority_fee_per_gas_len: B5,
    max_fee_per_gas_len: B5,
    gas_limit_len: B3,
    value_len: B5,
    access_list_size_len: B3,
}

#[derive(Clone, Copy, Debug, TryFromPrimitive)]
#[repr(u8)]
enum MessageVersion {
    Legacy = 0,
    EIP2930 = 1,
    EIP1559 = 2,
}

#[derive(Clone, Educe, PartialEq, Eq)]
#[educe(Debug)]
pub enum Message {
    Legacy {
        chain_id: Option<ChainId>,
        nonce: u64,
        gas_price: U256,
        gas_limit: u64,
        action: TransactionAction,
        value: U256,
        #[educe(Debug(method = "write_hex_string"))]
        input: Bytes,
    },
    EIP2930 {
        chain_id: ChainId,
        nonce: u64,
        gas_price: U256,
        gas_limit: u64,
        action: TransactionAction,
        value: U256,
        #[educe(Debug(method = "write_hex_string"))]
        input: Bytes,
        access_list: Vec<AccessListItem>,
    },
    EIP1559 {
        chain_id: ChainId,
        nonce: u64,
        max_priority_fee_per_gas: U256,
        max_fee_per_gas: U256,
        gas_limit: u64,
        action: TransactionAction,
        value: U256,
        #[educe(Debug(method = "write_hex_string"))]
        input: Bytes,
        access_list: Vec<AccessListItem>,
    },
}

impl Message {
    pub fn hash(&self) -> H256 {
        let mut buf = BytesMut::new();
        match self {
            Message::Legacy {
                chain_id,
                nonce,
                gas_price,
                gas_limit,
                action,
                value,
                input,
            } => {
                if let Some(chain_id) = chain_id {
                    #[derive(RlpEncodable)]
                    struct S<'a> {
                        nonce: u64,
                        gas_price: &'a U256,
                        gas_limit: u64,
                        action: &'a TransactionAction,
                        value: &'a U256,
                        input: &'a Bytes,
                        chain_id: ChainId,
                        _a: u8,
                        _b: u8,
                    }

                    S {
                        nonce: *nonce,
                        gas_price,
                        gas_limit: *gas_limit,
                        action,
                        value,
                        input,
                        chain_id: *chain_id,
                        _a: 0,
                        _b: 0,
                    }
                    .encode(&mut buf);
                } else {
                    #[derive(RlpEncodable)]
                    struct S<'a> {
                        nonce: u64,
                        gas_price: &'a U256,
                        gas_limit: u64,
                        action: &'a TransactionAction,
                        value: &'a U256,
                        input: &'a Bytes,
                    }

                    S {
                        nonce: *nonce,
                        gas_price,
                        gas_limit: *gas_limit,
                        action,
                        value,
                        input,
                    }
                    .encode(&mut buf);
                }
            }
            Message::EIP2930 {
                chain_id,
                nonce,
                gas_price,
                gas_limit,
                action,
                value,
                input,
                access_list,
            } => {
                buf.put_u8(1);

                #[derive(RlpEncodable)]
                struct S<'a> {
                    chain_id: ChainId,
                    nonce: u64,
                    gas_price: &'a U256,
                    gas_limit: u64,
                    action: &'a TransactionAction,
                    value: &'a U256,
                    input: &'a Bytes,
                    access_list: &'a Vec<AccessListItem>,
                }

                S {
                    chain_id: *chain_id,
                    nonce: *nonce,
                    gas_price,
                    gas_limit: *gas_limit,
                    action,
                    value,
                    input,
                    access_list,
                }
                .encode(&mut buf);
            }
            Message::EIP1559 {
                chain_id,
                nonce,
                max_priority_fee_per_gas,
                max_fee_per_gas,
                gas_limit,
                action,
                value,
                input,
                access_list,
            } => {
                buf.put_u8(2);

                #[derive(RlpEncodable)]
                struct S<'a> {
                    chain_id: ChainId,
                    nonce: u64,
                    max_priority_fee_per_gas: &'a U256,
                    max_fee_per_gas: &'a U256,
                    gas_limit: u64,
                    action: &'a TransactionAction,
                    value: &'a U256,
                    input: &'a Bytes,
                    access_list: &'a Vec<AccessListItem>,
                }

                S {
                    chain_id: *chain_id,
                    nonce: *nonce,
                    max_priority_fee_per_gas,
                    max_fee_per_gas,
                    gas_limit: *gas_limit,
                    action,
                    value,
                    input,
                    access_list,
                }
                .encode(&mut buf);
            }
        };

        keccak256(&buf)
    }
}

#[derive(Clone, Debug, Deref, PartialEq, Eq)]
pub struct MessageWithSignature {
    #[deref]
    pub message: Message,
    pub signature: MessageSignature,
}

#[derive(Clone, Debug, Deref, PartialEq, Eq)]
pub struct MessageWithSender {
    #[deref]
    pub message: Message,
    pub sender: Address,
}

fn signature_from_compact(
    mut buf: &[u8],
    v_len: u8,
) -> anyhow::Result<(MessageSignature, Option<ChainId>, &[u8])> {
    let (v, r, s);

    (v, buf) = variable_from_compact(buf, v_len)?;

    let YParityAndChainId {
        odd_y_parity,
        chain_id,
    } = YParityAndChainId::from_v(v).ok_or_else(|| format_err!("invalid v"))?;

    (r, buf) = h256_from_compact(buf)?;
    (s, buf) = h256_from_compact(buf)?;

    let signature = MessageSignature::new(odd_y_parity, r, s)
        .ok_or_else(|| format_err!("failed to reconstruct signature"))?;

    Ok((signature, chain_id, buf))
}

fn access_list_to_compact(buf: &mut impl BufMut, access_list: &[AccessListItem]) {
    let mut slots_num_buffer = unsigned_varint::encode::usize_buffer();
    for AccessListItem { address, slots } in access_list {
        buf.put_slice(&address[..]);
        buf.put_slice(unsigned_varint::encode::usize(
            slots.len(),
            &mut slots_num_buffer,
        ));
        for slot in slots {
            buf.put_slice(&slot[..]);
        }
    }
}

fn access_list_from_compact(
    mut buf: &[u8],
    size_len: u8,
) -> anyhow::Result<(Vec<AccessListItem>, &[u8])> {
    let access_list_size;
    (access_list_size, buf) = variable_from_compact(buf, size_len)?;

    // NOTE: using `Vec::with_capacity` requires extra care here because of potential panic on bad input.
    let mut access_list = Vec::new();
    for _ in 0..access_list_size {
        let address;
        (address, buf) = h160_from_compact(buf)?;

        let num_slots;
        (num_slots, buf) = unsigned_varint::decode::usize(buf)?;

        let mut slots = Vec::new();
        for _ in 0..num_slots {
            let slot;
            (slot, buf) = h256_from_compact(buf)?;

            slots.push(slot);
        }
        access_list.push(AccessListItem { address, slots });
    }

    Ok((access_list, buf))
}

impl MessageWithSignature {
    pub fn compact_encode(&self) -> Vec<u8> {
        match &self.message {
            Message::Legacy {
                chain_id,
                nonce,
                gas_price,
                gas_limit,
                action,
                value,
                input,
            } => {
                let mut flags = LegacyMessageFlags::default();
                flags.set_variant(MessageVersion::Legacy as u8);

                let v = YParityAndChainId {
                    chain_id: *chain_id,
                    odd_y_parity: self.signature.odd_y_parity,
                }
                .v();

                let v_encoded = variable_to_compact(v);
                flags.set_v_len(v_encoded.len() as u8);

                let nonce_encoded = variable_to_compact(*nonce);
                flags.set_nonce_len(nonce_encoded.len() as u8);

                let gas_price_encoded = variable_to_compact(*gas_price);
                flags.set_gas_price_len(gas_price_encoded.len() as u8);

                let gas_limit_encoded = variable_to_compact(*gas_limit);
                flags.set_gas_limit_len(gas_limit_encoded.len() as u8);

                let address = if let TransactionAction::Call(address) = action {
                    flags.set_is_call(true);
                    Some(*address)
                } else {
                    None
                };

                let value_encoded = variable_to_compact(*value);
                flags.set_value_len(value_encoded.len() as u8);

                let mut out = flags.into_bytes().to_vec();
                out.extend_from_slice(&v_encoded);
                out.extend_from_slice(&self.signature.r[..]);
                out.extend_from_slice(&self.signature.s[..]);
                out.extend_from_slice(&nonce_encoded);
                out.extend_from_slice(&gas_price_encoded);
                out.extend_from_slice(&gas_limit_encoded);

                if let Some(address) = address {
                    out.extend_from_slice(&address[..]);
                }

                out.extend_from_slice(&value_encoded);
                out.extend_from_slice(input);

                out
            }
            Message::EIP2930 {
                chain_id,
                nonce,
                gas_price,
                gas_limit,
                action,
                value,
                input,
                access_list,
            } => {
                let mut flags = EIP2930MessageFlags::default();
                flags.set_variant(MessageVersion::EIP2930 as u8);

                let v = YParityAndChainId {
                    chain_id: Some(*chain_id),
                    odd_y_parity: self.signature.odd_y_parity,
                }
                .v();

                let v_encoded = variable_to_compact(v);
                flags.set_v_len(v_encoded.len() as u8);

                let nonce_encoded = variable_to_compact(*nonce);
                flags.set_nonce_len(nonce_encoded.len() as u8);

                let gas_price_encoded = variable_to_compact(*gas_price);
                flags.set_gas_price_len(gas_price_encoded.len() as u8);

                let gas_limit_encoded = variable_to_compact(*gas_limit);
                flags.set_gas_limit_len(gas_limit_encoded.len() as u8);

                let address = if let TransactionAction::Call(address) = action {
                    flags.set_is_call(true);
                    Some(*address)
                } else {
                    None
                };

                let value_encoded = variable_to_compact(*value);
                flags.set_value_len(value_encoded.len() as u8);

                let access_list_size_encoded = variable_to_compact(access_list.len());
                flags.set_access_list_size_len(access_list_size_encoded.len() as u8);

                let mut out = flags.into_bytes().to_vec();
                out.extend_from_slice(&v_encoded);
                out.extend_from_slice(&self.signature.r[..]);
                out.extend_from_slice(&self.signature.s[..]);
                out.extend_from_slice(&nonce_encoded);
                out.extend_from_slice(&gas_price_encoded);
                out.extend_from_slice(&gas_limit_encoded);

                if let Some(address) = address {
                    out.extend_from_slice(&address[..]);
                }

                out.extend_from_slice(&value_encoded);

                out.extend_from_slice(&access_list_size_encoded);
                access_list_to_compact(&mut out, access_list);

                out.extend_from_slice(input);

                out
            }
            Message::EIP1559 {
                chain_id,
                nonce,
                max_priority_fee_per_gas,
                max_fee_per_gas,
                gas_limit,
                action,
                value,
                input,
                access_list,
            } => {
                let mut flags = EIP1559MessageFlags::default();
                flags.set_variant(MessageVersion::EIP1559 as u8);

                let v = YParityAndChainId {
                    chain_id: Some(*chain_id),
                    odd_y_parity: self.signature.odd_y_parity,
                }
                .v();

                let v_encoded = variable_to_compact(v);
                flags.set_v_len(v_encoded.len() as u8);

                let nonce_encoded = variable_to_compact(*nonce);
                flags.set_nonce_len(nonce_encoded.len() as u8);

                let max_priority_fee_per_gas_encoded =
                    variable_to_compact(*max_priority_fee_per_gas);
                flags
                    .set_max_priority_fee_per_gas_len(max_priority_fee_per_gas_encoded.len() as u8);

                let max_fee_per_gas_encoded = variable_to_compact(*max_fee_per_gas);
                flags.set_max_fee_per_gas_len(max_fee_per_gas_encoded.len() as u8);

                let gas_limit_encoded = variable_to_compact(*gas_limit);
                flags.set_gas_limit_len(gas_limit_encoded.len() as u8);

                let address = if let TransactionAction::Call(address) = action {
                    flags.set_is_call(true);
                    Some(*address)
                } else {
                    None
                };

                let value_encoded = variable_to_compact(*value);
                flags.set_value_len(value_encoded.len() as u8);

                let access_list_size_encoded = variable_to_compact(access_list.len());
                flags.set_access_list_size_len(access_list_size_encoded.len() as u8);

                let mut out = flags.into_bytes().to_vec();
                out.extend_from_slice(&v_encoded);
                out.extend_from_slice(&self.signature.r[..]);
                out.extend_from_slice(&self.signature.s[..]);
                out.extend_from_slice(&nonce_encoded);
                out.extend_from_slice(&max_priority_fee_per_gas_encoded);
                out.extend_from_slice(&max_fee_per_gas_encoded);
                out.extend_from_slice(&gas_limit_encoded);

                if let Some(address) = address {
                    out.extend_from_slice(&address[..]);
                }

                out.extend_from_slice(&value_encoded);

                out.extend_from_slice(&access_list_size_encoded);
                access_list_to_compact(&mut out, access_list);

                out.extend_from_slice(input);

                out
            }
        }
    }

    fn rlp_encode_inner(&self, out: &mut dyn BufMut, standalone: bool) {
        match &self.message {
            Message::Legacy {
                chain_id,
                nonce,
                gas_price,
                gas_limit,
                action,
                value,
                input,
            } => {
                #[derive(RlpEncodable)]
                struct S<'a> {
                    nonce: &'a u64,
                    gas_price: &'a U256,
                    gas_limit: &'a u64,
                    action: &'a TransactionAction,
                    value: &'a U256,
                    input: &'a Bytes,
                    v: u64,
                    r: U256,
                    s: U256,
                }

                S {
                    nonce,
                    gas_price,
                    gas_limit,
                    action,
                    value,
                    input,
                    v: YParityAndChainId {
                        odd_y_parity: self.signature.odd_y_parity,
                        chain_id: *chain_id,
                    }
                    .v(),
                    r: U256::from_be_bytes(self.signature.r.0),
                    s: U256::from_be_bytes(self.signature.s.0),
                }
                .encode(out);
            }
            Message::EIP2930 {
                chain_id,
                nonce,
                gas_price,
                gas_limit,
                action,
                value,
                input,
                access_list,
            } => {
                #[derive(RlpEncodable)]
                struct S<'a> {
                    chain_id: &'a ChainId,
                    nonce: &'a u64,
                    gas_price: &'a U256,
                    gas_limit: &'a u64,
                    action: &'a TransactionAction,
                    value: &'a U256,
                    input: &'a Bytes,
                    access_list: &'a Vec<AccessListItem>,
                    odd_y_parity: bool,
                    r: U256,
                    s: U256,
                }

                let s = S {
                    chain_id,
                    nonce,
                    gas_price,
                    gas_limit,
                    action,
                    value,
                    input,
                    access_list,
                    odd_y_parity: self.signature.odd_y_parity,
                    r: U256::from_be_bytes(self.signature.r.0),
                    s: U256::from_be_bytes(self.signature.s.0),
                };

                if standalone {
                    out.put_u8(1);
                    s.encode(out);
                } else {
                    let mut tmp = BytesMut::new();
                    tmp.put_u8(1);
                    s.encode(&mut tmp);

                    Encodable::encode(&(&*tmp as &[u8]), out);
                }
            }
            Message::EIP1559 {
                chain_id,
                nonce,
                max_priority_fee_per_gas,
                max_fee_per_gas,
                gas_limit,
                action,
                value,
                input,
                access_list,
            } => {
                #[derive(RlpEncodable)]
                struct S<'a> {
                    chain_id: &'a ChainId,
                    nonce: &'a u64,
                    max_priority_fee_per_gas: &'a U256,
                    max_fee_per_gas: &'a U256,
                    gas_limit: &'a u64,
                    action: &'a TransactionAction,
                    value: &'a U256,
                    input: &'a Bytes,
                    access_list: &'a Vec<AccessListItem>,
                    odd_y_parity: bool,
                    r: U256,
                    s: U256,
                }

                let s = S {
                    chain_id,
                    nonce,
                    max_priority_fee_per_gas,
                    max_fee_per_gas,
                    gas_limit,
                    action,
                    value,
                    input,
                    access_list,
                    odd_y_parity: self.signature.odd_y_parity,
                    r: U256::from_be_bytes(self.signature.r.0),
                    s: U256::from_be_bytes(self.signature.s.0),
                };

                if standalone {
                    out.put_u8(2);
                    s.encode(out);
                } else {
                    let mut tmp = BytesMut::new();
                    tmp.put_u8(2);
                    s.encode(&mut tmp);

                    Encodable::encode(&(&*tmp as &[u8]), out);
                }
            }
        }
    }

    pub fn compact_decode(mut buf: &[u8]) -> anyhow::Result<Self> {
        if buf.is_empty() {
            bail!("input too short");
        }

        let flags = MessageVariant::from_bytes([buf[0]]);

        match MessageVersion::try_from(flags.version())? {
            MessageVersion::Legacy => {
                if buf.len() < 3 {
                    bail!("input too short");
                }

                let flags =
                    LegacyMessageFlags::from_bytes([buf.get_u8(), buf.get_u8(), buf.get_u8()]);

                let (signature, chain_id);
                (signature, chain_id, buf) = signature_from_compact(buf, flags.v_len())?;

                let nonce;
                (nonce, buf) = variable_from_compact(buf, flags.nonce_len())?;

                let gas_price;
                (gas_price, buf) = variable_from_compact(buf, flags.gas_price_len())?;

                let gas_limit;
                (gas_limit, buf) = variable_from_compact(buf, flags.gas_limit_len())?;

                let action = if flags.is_call() {
                    let item;
                    (item, buf) = h160_from_compact(buf)?;
                    TransactionAction::Call(item)
                } else {
                    TransactionAction::Create
                };

                let value;
                (value, buf) = variable_from_compact(buf, flags.value_len())?;

                let input = buf[..].to_vec().into();

                Ok(Self {
                    message: Message::Legacy {
                        chain_id,
                        nonce,
                        gas_price,
                        gas_limit,
                        action,
                        value,
                        input,
                    },
                    signature,
                })
            }
            MessageVersion::EIP2930 => {
                if buf.len() < 4 {
                    bail!("input too short");
                }

                let flags = EIP2930MessageFlags::from_bytes([
                    buf.get_u8(),
                    buf.get_u8(),
                    buf.get_u8(),
                    buf.get_u8(),
                ]);

                let (signature, chain_id);
                (signature, chain_id, buf) = signature_from_compact(buf, flags.v_len())?;

                let chain_id = chain_id.ok_or_else(|| {
                    format_err!("ChainId is only be optional for legacy transactiosn")
                })?;

                let nonce;
                (nonce, buf) = variable_from_compact(buf, flags.nonce_len())?;

                let gas_price;
                (gas_price, buf) = variable_from_compact(buf, flags.gas_price_len())?;

                let gas_limit;
                (gas_limit, buf) = variable_from_compact(buf, flags.gas_limit_len())?;

                let action = if flags.is_call() {
                    let item;
                    (item, buf) = h160_from_compact(buf)?;
                    TransactionAction::Call(item)
                } else {
                    TransactionAction::Create
                };

                let value;
                (value, buf) = variable_from_compact(buf, flags.value_len())?;

                let access_list;
                (access_list, buf) = access_list_from_compact(buf, flags.access_list_size_len())?;

                let input = buf[..].to_vec().into();

                Ok(Self {
                    message: Message::EIP2930 {
                        chain_id,
                        nonce,
                        gas_price,
                        gas_limit,
                        action,
                        value,
                        access_list,
                        input,
                    },
                    signature,
                })
            }
            MessageVersion::EIP1559 => {
                if buf.len() < 4 {
                    bail!("input too short");
                }

                let flags = EIP1559MessageFlags::from_bytes([
                    buf.get_u8(),
                    buf.get_u8(),
                    buf.get_u8(),
                    buf.get_u8(),
                ]);

                let (signature, chain_id);
                (signature, chain_id, buf) = signature_from_compact(buf, flags.v_len())?;

                let chain_id = chain_id.ok_or_else(|| {
                    format_err!("ChainId is only be optional for legacy transactiosn")
                })?;

                let nonce;
                (nonce, buf) = variable_from_compact(buf, flags.nonce_len())?;

                let max_priority_fee_per_gas;
                (max_priority_fee_per_gas, buf) =
                    variable_from_compact(buf, flags.max_priority_fee_per_gas_len())?;

                let max_fee_per_gas;
                (max_fee_per_gas, buf) = variable_from_compact(buf, flags.max_fee_per_gas_len())?;

                let gas_limit;
                (gas_limit, buf) = variable_from_compact(buf, flags.gas_limit_len())?;

                let action = if flags.is_call() {
                    let item;
                    (item, buf) = h160_from_compact(buf)?;
                    TransactionAction::Call(item)
                } else {
                    TransactionAction::Create
                };

                let value;
                (value, buf) = variable_from_compact(buf, flags.value_len())?;

                let access_list;
                (access_list, buf) = access_list_from_compact(buf, flags.access_list_size_len())?;

                let input = buf[..].to_vec().into();

                Ok(Self {
                    message: Message::EIP1559 {
                        chain_id,
                        nonce,
                        max_priority_fee_per_gas,
                        max_fee_per_gas,
                        gas_limit,
                        action,
                        value,
                        access_list,
                        input,
                    },
                    signature,
                })
            }
        }
    }
}

impl TrieEncode for MessageWithSignature {
    fn trie_encode(&self, buf: &mut dyn BufMut) {
        self.rlp_encode_inner(buf, true)
    }
}

impl Encodable for MessageWithSignature {
    fn encode(&self, out: &mut dyn BufMut) {
        self.rlp_encode_inner(out, false)
    }
}

impl Decodable for MessageWithSignature {
    fn decode(buf: &mut &[u8]) -> Result<Self, DecodeError> {
        let s = Self::rlp_decode(buf)?;

        if s.max_fee_per_gas()
            .checked_mul(s.gas_limit().into())
            .is_none()
        {
            return Err(DecodeError::Custom("gas limit price product overflow"));
        }

        Ok(s)
    }
}

impl MessageWithSignature {
    fn rlp_decode(buf: &mut &[u8]) -> Result<Self, DecodeError> {
        let h = Header::decode(&mut &**buf)?;

        if h.list {
            #[derive(RlpDecodable)]
            struct S {
                nonce: u64,
                gas_price: U256,
                gas_limit: u64,
                action: TransactionAction,
                value: U256,
                input: Bytes,
                v: u64,
                r: U256,
                s: U256,
            }

            let S {
                nonce,
                gas_price,
                gas_limit,
                action,
                value,
                input,
                v,
                r,
                s,
            } = S::decode(buf)?;

            let YParityAndChainId {
                odd_y_parity,
                chain_id,
            } = YParityAndChainId::from_v(v).ok_or(DecodeError::Custom("Invalid recovery ID"))?;
            let signature = MessageSignature::new(odd_y_parity, u256_to_h256(r), u256_to_h256(s))
                .ok_or(DecodeError::Custom("Invalid transaction signature format"))?;

            return Ok(Self {
                message: Message::Legacy {
                    chain_id,
                    nonce,
                    gas_price,
                    gas_limit,
                    action,
                    value,
                    input,
                },
                signature,
            });
        }

        Header::decode(buf)?;

        if buf.is_empty() {
            return Err(DecodeError::Custom("no tx body"));
        }

        let first = buf.get_u8();

        if first == 0x01 {
            #[derive(RlpDecodable)]
            struct S {
                chain_id: ChainId,
                nonce: u64,
                gas_price: U256,
                gas_limit: u64,
                action: TransactionAction,
                value: U256,
                input: Bytes,
                access_list: Vec<AccessListItem>,
                odd_y_parity: bool,
                r: U256,
                s: U256,
            }

            let S {
                chain_id,
                nonce,
                gas_price,
                gas_limit,
                action,
                value,
                input,
                access_list,
                odd_y_parity,
                r,
                s,
            } = S::decode(buf)?;

            return Ok(Self {
                message: Message::EIP2930 {
                    chain_id,
                    nonce,
                    gas_price,
                    gas_limit,
                    action,
                    value,
                    input,
                    access_list,
                },
                signature: MessageSignature::new(odd_y_parity, u256_to_h256(r), u256_to_h256(s))
                    .ok_or(DecodeError::Custom("Invalid transaction signature format"))?,
            });
        }

        if first == 0x02 {
            #[derive(RlpDecodable)]
            struct S {
                chain_id: ChainId,
                nonce: u64,
                max_priority_fee_per_gas: U256,
                max_fee_per_gas: U256,
                gas_limit: u64,
                action: TransactionAction,
                value: U256,
                input: Bytes,
                access_list: Vec<AccessListItem>,
                odd_y_parity: bool,
                r: U256,
                s: U256,
            }

            let S {
                chain_id,
                nonce,
                max_priority_fee_per_gas,
                max_fee_per_gas,
                gas_limit,
                action,
                value,
                input,
                access_list,
                odd_y_parity,
                r,
                s,
            } = S::decode(buf)?;

            return Ok(Self {
                message: Message::EIP1559 {
                    chain_id,
                    nonce,
                    max_priority_fee_per_gas,
                    max_fee_per_gas,
                    gas_limit,
                    action,
                    value,
                    input,
                    access_list,
                },
                signature: MessageSignature::new(odd_y_parity, u256_to_h256(r), u256_to_h256(s))
                    .ok_or(DecodeError::Custom("Invalid transaction signature format"))?,
            });
        }

        Err(DecodeError::Custom("invalid tx type"))
    }
}

impl Message {
    pub const fn tx_type(&self) -> TxType {
        match self {
            Self::Legacy { .. } => TxType::Legacy,
            Self::EIP2930 { .. } => TxType::EIP2930,
            Self::EIP1559 { .. } => TxType::EIP1559,
        }
    }

    pub fn chain_id(&self) -> Option<ChainId> {
        match *self {
            Self::Legacy { chain_id, .. } => chain_id,
            Self::EIP2930 { chain_id, .. } => Some(chain_id),
            Self::EIP1559 { chain_id, .. } => Some(chain_id),
        }
    }

    pub const fn nonce(&self) -> u64 {
        match *self {
            Self::Legacy { nonce, .. }
            | Self::EIP2930 { nonce, .. }
            | Self::EIP1559 { nonce, .. } => nonce,
        }
    }

    pub const fn max_priority_fee_per_gas(&self) -> U256 {
        match *self {
            Self::Legacy { gas_price, .. } | Self::EIP2930 { gas_price, .. } => gas_price,
            Self::EIP1559 {
                max_priority_fee_per_gas,
                ..
            } => max_priority_fee_per_gas,
        }
    }

    pub const fn max_fee_per_gas(&self) -> U256 {
        match *self {
            Self::Legacy { gas_price, .. } | Self::EIP2930 { gas_price, .. } => gas_price,
            Self::EIP1559 {
                max_fee_per_gas, ..
            } => max_fee_per_gas,
        }
    }

    pub const fn gas_limit(&self) -> u64 {
        match *self {
            Self::Legacy { gas_limit, .. }
            | Self::EIP2930 { gas_limit, .. }
            | Self::EIP1559 { gas_limit, .. } => gas_limit,
        }
    }

    pub const fn action(&self) -> TransactionAction {
        match *self {
            Self::Legacy { action, .. }
            | Self::EIP2930 { action, .. }
            | Self::EIP1559 { action, .. } => action,
        }
    }

    pub const fn value(&self) -> U256 {
        match *self {
            Self::Legacy { value, .. }
            | Self::EIP2930 { value, .. }
            | Self::EIP1559 { value, .. } => value,
        }
    }

    pub const fn input(&self) -> &Bytes {
        match self {
            Self::Legacy { input, .. }
            | Self::EIP2930 { input, .. }
            | Self::EIP1559 { input, .. } => input,
        }
    }

    pub const fn access_list(&self) -> Cow<'_, AccessList> {
        match self {
            Self::Legacy { .. } => Cow::Owned(AccessList::new()),
            Self::EIP2930 { access_list, .. } | Self::EIP1559 { access_list, .. } => {
                Cow::Borrowed(access_list)
            }
        }
    }

    pub(crate) fn priority_fee_per_gas(&self, base_fee_per_gas: U256) -> Option<U256> {
        self.max_fee_per_gas()
            .checked_sub(base_fee_per_gas)
            .map(|v| std::cmp::min(self.max_priority_fee_per_gas(), v))
    }

    pub(crate) fn effective_gas_price(&self, base_fee_per_gas: U256) -> Option<U256> {
        self.priority_fee_per_gas(base_fee_per_gas)
            .map(|v| v + base_fee_per_gas)
    }
}

impl MessageWithSignature {
    pub fn hash(&self) -> H256 {
        let mut buf = BytesMut::new();
        self.trie_encode(&mut buf);
        keccak256(buf)
    }

    pub fn v(&self) -> u64 {
        YParityAndChainId {
            odd_y_parity: self.signature.odd_y_parity,
            chain_id: self.chain_id(),
        }
        .v()
    }

    pub fn r(&self) -> H256 {
        self.signature.r
    }

    pub fn s(&self) -> H256 {
        self.signature.s
    }

    pub fn recover_sender(&self) -> anyhow::Result<Address> {
        let mut sig = [0u8; 64];

        sig[..32].copy_from_slice(self.r().as_bytes());
        sig[32..].copy_from_slice(self.s().as_bytes());

        let rec = RecoveryId::from_i32(self.signature.odd_y_parity() as i32)?;

        let public = &SECP256K1.recover_ecdsa(
            &SecpMessage::from_slice(self.message.hash().as_bytes())?,
            &RecoverableSignature::from_compact(&sig, rec)?,
        )?;

        let address_slice = &Keccak256::digest(&public.serialize_uncompressed()[1..])[12..];
        Ok(Address::from_slice(address_slice))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hex_literal::hex;

    #[test]
    fn can_decode_raw_transaction() {
        let bytes = &hex!("f901e48080831000008080b90196608060405234801561001057600080fd5b50336000806101000a81548173ffffffffffffffffffffffffffffffffffffffff021916908373ffffffffffffffffffffffffffffffffffffffff1602179055507fc68045c3c562488255b55aa2c4c7849de001859ff0d8a36a75c2d5ed80100fb660405180806020018281038252600d8152602001807f48656c6c6f2c20776f726c64210000000000000000000000000000000000000081525060200191505060405180910390a160cf806100c76000396000f3fe6080604052348015600f57600080fd5b506004361060285760003560e01c80638da5cb5b14602d575b600080fd5b60336075565b604051808273ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200191505060405180910390f35b6000809054906101000a900473ffffffffffffffffffffffffffffffffffffffff168156fea265627a7a72315820fae816ad954005c42bea7bc7cb5b19f7fd5d3a250715ca2023275c9ca7ce644064736f6c634300050f003278a04cab43609092a99cf095d458b61b47189d1bbab64baed10a0fd7b7d2de2eb960a011ab1bcda76dfed5e733219beb83789f9887b2a7b2e61759c7c90f7d40403201") as &[u8];

        let buf = &mut &*bytes;
        <MessageWithSignature as Decodable>::decode(buf).unwrap();
        assert!(buf.is_empty());
    }

    fn check_transaction(v: &MessageWithSignature, standalone_idx: usize) {
        let mut encoded = BytesMut::new();
        Encodable::encode(&v, &mut encoded);

        let encoded_view = &mut &*encoded;
        let decoded = <MessageWithSignature as Decodable>::decode(encoded_view).unwrap();
        assert!(encoded_view.is_empty());

        assert_eq!(decoded, *v);
        let mut consensus_encoded = BytesMut::new();
        v.trie_encode(&mut consensus_encoded);
        assert_eq!(encoded[standalone_idx..], consensus_encoded);

        let compact_encoded = v.compact_encode();
        let compact_decoded = MessageWithSignature::compact_decode(&compact_encoded).unwrap();

        assert_eq!(*v, compact_decoded);
    }

    #[test]
    fn transaction_legacy() {
        let v = MessageWithSignature {
            message: Message::Legacy {
                chain_id: Some(ChainId(2)),
                nonce: 12,
                gas_price: 20_000_000_000_u64.into(),
                gas_limit: 21000,
                action: TransactionAction::Call(
                    hex!("727fc6a68321b754475c668a6abfb6e9e71c169a").into(),
                ),
                value: 10.as_u256() * 1_000_000_000 * 1_000_000_000,
                input: hex!("a9059cbb000000000213ed0f886efd100b67c7e4ec0a85a7d20dc971600000000000000000000015af1d78b58c4000").to_vec().into(),
            },
			signature: MessageSignature::new(
                true,
                hex!("be67e0a07db67da8d446f76add590e54b6e92cb6b8f9835aeb67540579a27717"),
                hex!("2d690516512020171c1ec870f6ff45398cc8609250326be89915fb538e7bd718"),
            ).unwrap(),
		};

        check_transaction(&v, 0);
    }

    #[test]
    fn transaction_eip2930() {
        let v = MessageWithSignature {
            message: Message::EIP2930 {
                chain_id: ChainId(5),
                nonce: 7,
                gas_price: 30_000_000_000_u64.into(),
                gas_limit: 5_748_100_u64,
                action: TransactionAction::Call(
                    hex!("811a752c8cd697e3cb27279c330ed1ada745a8d7").into(),
                ),
                value: 2.as_u256() * 1_000_000_000 * 1_000_000_000,
                input: hex!("6ebaf477f83e051589c1188bcc6ddccd").to_vec().into(),
                access_list: vec![
                    AccessListItem {
                        address: hex!("de0b295669a9fd93d5f28d9ec85e40f4cb697bae").into(),
                        slots: vec![
                            hex!(
                                "0000000000000000000000000000000000000000000000000000000000000003"
                            )
                            .into(),
                            hex!(
                                "0000000000000000000000000000000000000000000000000000000000000007"
                            )
                            .into(),
                        ],
                    },
                    AccessListItem {
                        address: hex!("bb9bc244d798123fde783fcc1c72d3bb8c189413").into(),
                        slots: vec![],
                    },
                ],
            },
            signature: MessageSignature::new(
                false,
                hex!("36b241b061a36a32ab7fe86c7aa9eb592dd59018cd0443adc0903590c16b02b0"),
                hex!("5edcc541b4741c5cc6dd347c5ed9577ef293a62787b4510465fadbfe39ee4094"),
            )
            .unwrap(),
        };

        check_transaction(&v, 2);
    }

    #[test]
    fn transaction_eip1559() {
        let v =
            MessageWithSignature {
                message: Message::EIP1559 {
                    chain_id: ChainId(5),
                    nonce: 7,
                    max_priority_fee_per_gas: 10_000_000_000_u64.into(),
                    max_fee_per_gas: 30_000_000_000_u64.into(),
                    gas_limit: 5_748_100_u64,
                    action: TransactionAction::Call(
                        hex!("811a752c8cd697e3cb27279c330ed1ada745a8d7").into(),
                    ),
                    value: 2.as_u256() * 1_000_000_000 * 1_000_000_000,
                    input: hex!("6ebaf477f83e051589c1188bcc6ddccd").to_vec().into(),
                    access_list: vec![
                        AccessListItem {
                            address: hex!("de0b295669a9fd93d5f28d9ec85e40f4cb697bae").into(),
                            slots: vec![
                        hex!("0000000000000000000000000000000000000000000000000000000000000003")
                            .into(),
                        hex!("0000000000000000000000000000000000000000000000000000000000000007")
                            .into(),
                    ],
                        },
                        AccessListItem {
                            address: hex!("bb9bc244d798123fde783fcc1c72d3bb8c189413").into(),
                            slots: vec![],
                        },
                    ],
                },
                signature: MessageSignature::new(
                    false,
                    hex!("36b241b061a36a32ab7fe86c7aa9eb592dd59018cd0443adc0903590c16b02b0"),
                    hex!("5edcc541b4741c5cc6dd347c5ed9577ef293a62787b4510465fadbfe39ee4094"),
                )
                .unwrap(),
            };

        check_transaction(&v, 2);
    }

    #[test]
    fn y_parity_and_chain_id() {
        for range in [0..27, 29..35] {
            for v in range {
                assert_eq!(YParityAndChainId::from_v(v), None);
            }
        }
        assert_eq!(
            YParityAndChainId::from_v(27).unwrap(),
            YParityAndChainId {
                odd_y_parity: false,
                chain_id: None
            }
        );
        assert_eq!(
            YParityAndChainId::from_v(28).unwrap(),
            YParityAndChainId {
                odd_y_parity: true,
                chain_id: None
            }
        );

        assert_eq!(
            YParityAndChainId::from_v(35).unwrap(),
            YParityAndChainId {
                odd_y_parity: false,
                chain_id: Some(ChainId(0))
            }
        );
        assert_eq!(
            YParityAndChainId::from_v(36).unwrap(),
            YParityAndChainId {
                odd_y_parity: true,
                chain_id: Some(ChainId(0))
            }
        );
        assert_eq!(
            YParityAndChainId::from_v(37).unwrap(),
            YParityAndChainId {
                odd_y_parity: false,
                chain_id: Some(ChainId(1))
            }
        );
        assert_eq!(
            YParityAndChainId::from_v(38).unwrap(),
            YParityAndChainId {
                odd_y_parity: true,
                chain_id: Some(ChainId(1))
            }
        );

        assert_eq!(
            YParityAndChainId {
                odd_y_parity: false,
                chain_id: None
            }
            .v(),
            27
        );
        assert_eq!(
            YParityAndChainId {
                odd_y_parity: true,
                chain_id: None
            }
            .v(),
            28
        );
        assert_eq!(
            YParityAndChainId {
                odd_y_parity: false,
                chain_id: Some(ChainId(1))
            }
            .v(),
            37
        );
        assert_eq!(
            YParityAndChainId {
                odd_y_parity: true,
                chain_id: Some(ChainId(1))
            }
            .v(),
            38
        );
    }
}
