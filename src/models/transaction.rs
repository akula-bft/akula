use crate::{
    crypto::{is_valid_signature, TrieEncode},
    models::*,
    util::*,
};
use bytes::{BufMut, Bytes, BytesMut};
use derive_more::Deref;
use educe::Educe;
use hex_literal::hex;
use parity_scale_codec::{Compact, Decode, Encode, EncodeAsRef, EncodeLike, Input};
use rlp::{Decodable, DecoderError, Encodable, Rlp, RlpStream};
use secp256k1::{
    ecdsa::{RecoverableSignature, RecoveryId},
    Message as SecpMessage, SECP256K1,
};
use serde::*;
use sha3::*;
use std::{borrow::Cow, cmp::min};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum TxType {
    Legacy = 0,
    EIP2930 = 1,
    EIP1559 = 2,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, Encode, Decode)]
pub enum TransactionAction {
    Call(Address),
    Create,
}

impl Encodable for TransactionAction {
    fn rlp_append(&self, s: &mut RlpStream) {
        match self {
            Self::Call(address) => {
                s.encoder().encode_value(&address[..]);
            }
            Self::Create => s.encoder().encode_value(&[]),
        }
    }
}

impl Decodable for TransactionAction {
    fn decode(rlp: &Rlp) -> Result<Self, DecoderError> {
        if rlp.is_empty() {
            if rlp.is_data() {
                Ok(TransactionAction::Create)
            } else {
                Err(DecoderError::RlpExpectedToBeData)
            }
        } else {
            Ok(TransactionAction::Call(rlp.as_val()?))
        }
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Encode, Decode)]
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Encode, Decode)]
pub struct AccessListItem {
    pub address: Address,
    pub slots: Vec<H256>,
}

impl Encodable for AccessListItem {
    fn rlp_append(&self, s: &mut RlpStream) {
        s.begin_list(2);
        s.append(&self.address);
        s.append_list(&self.slots);
    }
}

impl Decodable for AccessListItem {
    fn decode(rlp: &Rlp) -> Result<Self, DecoderError> {
        Ok(Self {
            address: rlp.val_at(0)?,
            slots: rlp.list_at(1)?,
        })
    }
}

pub type AccessList = Vec<AccessListItem>;

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct OptionalChainId(pub Option<ChainId>);

impl Encode for OptionalChainId {
    fn using_encoded<R, F: FnOnce(&[u8]) -> R>(&self, f: F) -> R {
        Compact(if let Some(chainid) = self.0 {
            chainid.0
        } else {
            0
        })
        .using_encoded(f)
    }
}

impl Decode for OptionalChainId {
    fn decode<I: Input>(input: &mut I) -> Result<Self, parity_scale_codec::Error> {
        Compact::<u64>::decode(input).map(|input| {
            OptionalChainId(if input.0 == 0 {
                None
            } else {
                Some(ChainId(input.0))
            })
        })
    }
}

impl EncodeLike for OptionalChainId {}

impl From<OptionalChainId> for Option<ChainId> {
    fn from(v: OptionalChainId) -> Self {
        v.0
    }
}

impl EncodeAsRef<'_, Option<ChainId>> for OptionalChainId {
    type RefType = OptionalChainId;
}

impl<'a> From<&'a Option<ChainId>> for OptionalChainId {
    fn from(c: &'a Option<ChainId>) -> Self {
        Self(*c)
    }
}

#[derive(Clone, Educe, PartialEq, Eq, Serialize, Deserialize, Encode, Decode)]
#[educe(Debug)]
pub enum Message {
    Legacy {
        #[codec(encoded_as = "OptionalChainId")]
        chain_id: Option<ChainId>,
        #[codec(compact)]
        nonce: u64,
        #[codec(compact)]
        gas_price: U256,
        #[codec(compact)]
        gas_limit: u64,
        action: TransactionAction,
        #[codec(compact)]
        value: U256,
        #[educe(Debug(method = "write_hex_string"))]
        input: Bytes,
    },
    EIP2930 {
        #[codec(compact)]
        chain_id: ChainId,
        #[codec(compact)]
        nonce: u64,
        #[codec(compact)]
        gas_price: U256,
        #[codec(compact)]
        gas_limit: u64,
        action: TransactionAction,
        #[codec(compact)]
        value: U256,
        #[educe(Debug(method = "write_hex_string"))]
        input: Bytes,
        access_list: Vec<AccessListItem>,
    },
    EIP1559 {
        #[codec(compact)]
        chain_id: ChainId,
        #[codec(compact)]
        nonce: u64,
        #[codec(compact)]
        max_priority_fee_per_gas: U256,
        #[codec(compact)]
        max_fee_per_gas: U256,
        #[codec(compact)]
        gas_limit: u64,
        action: TransactionAction,
        #[codec(compact)]
        value: U256,
        #[educe(Debug(method = "write_hex_string"))]
        input: Bytes,
        access_list: Vec<AccessListItem>,
    },
}

impl Message {
    pub fn hash(&self) -> H256 {
        let msg = match self {
            Message::Legacy {
                chain_id,
                nonce,
                gas_price,
                gas_limit,
                action,
                value,
                input,
            } => {
                let mut s = RlpStream::new();
                if let Some(chain_id) = chain_id {
                    s.begin_list(9);
                    s.append(nonce);
                    s.append(gas_price);
                    s.append(gas_limit);
                    s.append(action);
                    s.append(value);
                    s.append(&input.as_ref());
                    s.append(chain_id);
                    s.append(&0_u8);
                    s.append(&0_u8);
                } else {
                    s.begin_list(6);
                    s.append(nonce);
                    s.append(gas_price);
                    s.append(gas_limit);
                    s.append(action);
                    s.append(value);
                    s.append(&input.as_ref());
                }
                s.out()
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
                let mut b = BytesMut::with_capacity(1);
                b.put_u8(1);
                let mut s = RlpStream::new_with_buffer(b);
                s.begin_list(8);
                s.append(chain_id);
                s.append(nonce);
                s.append(gas_price);
                s.append(gas_limit);
                s.append(action);
                s.append(value);
                s.append(&input.as_ref());
                s.append_list(access_list);
                s.out()
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
                let mut b = BytesMut::with_capacity(1);
                b.put_u8(2);
                let mut s = RlpStream::new_with_buffer(b);
                s.begin_list(9);
                s.append(chain_id);
                s.append(nonce);
                s.append(max_priority_fee_per_gas);
                s.append(max_fee_per_gas);
                s.append(gas_limit);
                s.append(action);
                s.append(value);
                s.append(&input.as_ref());
                s.append_list(access_list);
                s.out()
            }
        };

        H256::from_slice(Keccak256::digest(&msg.freeze()).as_slice())
    }
}

#[derive(Clone, Debug, Deref, PartialEq, Eq, Serialize, Deserialize, Encode, Decode)]
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

impl MessageWithSignature {
    fn encode_inner(&self, s: &mut RlpStream, standalone: bool) {
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
                s.begin_list(9);
                s.append(nonce);
                s.append(gas_price);
                s.append(gas_limit);
                s.append(action);
                s.append(value);
                s.append(&input.as_ref());
                s.append(
                    &YParityAndChainId {
                        odd_y_parity: self.signature.odd_y_parity,
                        chain_id: *chain_id,
                    }
                    .v(),
                );
                s.append(&U256::from_be_bytes(self.signature.r.0));
                s.append(&U256::from_be_bytes(self.signature.s.0));
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
                let mut b = BytesMut::with_capacity(1);
                b.put_u8(1);
                let mut s1 = RlpStream::new_list_with_buffer(b, 11);
                s1.append(chain_id);
                s1.append(nonce);
                s1.append(gas_price);
                s1.append(gas_limit);
                s1.append(action);
                s1.append(value);
                s1.append(&input.as_ref());
                s1.append_list(access_list);
                s1.append(&self.signature.odd_y_parity);
                s1.append(&U256::from_be_bytes(self.signature.r.0));
                s1.append(&U256::from_be_bytes(self.signature.s.0));
                if standalone {
                    s.append_raw(&*s1.out().freeze(), 1);
                } else {
                    s.append(&s1.out());
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
                let mut b = BytesMut::with_capacity(1);
                b.put_u8(2);
                let mut s1 = RlpStream::new_list_with_buffer(b, 12);
                s1.append(chain_id);
                s1.append(nonce);
                s1.append(max_priority_fee_per_gas);
                s1.append(max_fee_per_gas);
                s1.append(gas_limit);
                s1.append(action);
                s1.append(value);
                s1.append(&input.as_ref());
                s1.append_list(access_list);
                s1.append(&self.signature.odd_y_parity);
                s1.append(&U256::from_be_bytes(self.signature.r.0));
                s1.append(&U256::from_be_bytes(self.signature.s.0));
                if standalone {
                    s.append_raw(&*s1.out().freeze(), 1);
                } else {
                    s.append(&s1.out());
                }
            }
        }
    }
}

impl TrieEncode for MessageWithSignature {
    fn trie_encode(&self) -> Bytes {
        let mut s = RlpStream::new();
        self.encode_inner(&mut s, true);
        s.out().freeze()
    }
}

impl MessageWithSignature {
    pub fn trie_decode(slice: &[u8]) -> Result<MessageWithSignature, DecoderError> {
        let first = *slice.get(0).ok_or(DecoderError::Custom("empty slice"))?;

        if first == 0x01 {
            let s = slice.get(1..).ok_or(DecoderError::Custom("no tx body"))?;
            let rlp = Rlp::new(s);
            if rlp.item_count()? != 11 {
                return Err(DecoderError::RlpIncorrectListLen);
            }

            return Ok(Self {
                message: Message::EIP2930 {
                    chain_id: rlp.val_at(0)?,
                    nonce: rlp.val_at(1)?,
                    gas_price: rlp.val_at(2)?,
                    gas_limit: rlp.val_at(3)?,
                    action: rlp.val_at(4)?,
                    value: rlp.val_at(5)?,
                    input: rlp.val_at::<Vec<u8>>(6)?.into(),
                    access_list: rlp.list_at(7)?,
                },
                signature: MessageSignature::new(
                    rlp.val_at(8)?,
                    H256(rlp.val_at::<U256>(9)?.to_be_bytes()),
                    H256(rlp.val_at::<U256>(10)?.to_be_bytes()),
                )
                .ok_or(DecoderError::Custom("Invalid transaction signature format"))?,
            });
        }

        if first == 0x02 {
            let s = slice.get(1..).ok_or(DecoderError::Custom("no tx body"))?;
            let rlp = Rlp::new(s);
            if rlp.item_count()? != 12 {
                return Err(DecoderError::RlpIncorrectListLen);
            }

            return Ok(Self {
                message: Message::EIP1559 {
                    chain_id: rlp.val_at(0)?,
                    nonce: rlp.val_at(1)?,
                    max_priority_fee_per_gas: rlp.val_at(2)?,
                    max_fee_per_gas: rlp.val_at(3)?,
                    gas_limit: rlp.val_at(4)?,
                    action: rlp.val_at(5)?,
                    value: rlp.val_at(6)?,
                    input: rlp.val_at::<Vec<u8>>(7)?.into(),
                    access_list: rlp.list_at(8)?,
                },
                signature: MessageSignature::new(
                    rlp.val_at(9)?,
                    H256(rlp.val_at::<U256>(10)?.to_be_bytes()),
                    H256(rlp.val_at::<U256>(11)?.to_be_bytes()),
                )
                .ok_or(DecoderError::Custom("Invalid transaction signature format"))?,
            });
        }

        let rlp = Rlp::new(slice);
        if rlp.is_list() {
            if rlp.item_count()? != 9 {
                return Err(DecoderError::RlpIncorrectListLen);
            }

            let YParityAndChainId {
                odd_y_parity: odd,
                chain_id,
            } = YParityAndChainId::from_v(rlp.val_at(6)?)
                .ok_or(DecoderError::Custom("Invalid recovery ID"))?;
            let r = H256(rlp.val_at::<U256>(7)?.to_be_bytes());
            let s = H256(rlp.val_at::<U256>(8)?.to_be_bytes());
            let signature = MessageSignature::new(odd, r, s)
                .ok_or(DecoderError::Custom("Invalid transaction signature format"))?;

            return Ok(Self {
                message: Message::Legacy {
                    chain_id,
                    nonce: rlp.val_at(0)?,
                    gas_price: rlp.val_at(1)?,
                    gas_limit: rlp.val_at(2)?,
                    action: rlp.val_at(3)?,
                    value: rlp.val_at(4)?,
                    input: rlp.val_at::<Vec<u8>>(5)?.into(),
                },
                signature,
            });
        }

        Err(DecoderError::Custom("invalid tx type"))
    }
}

impl Encodable for MessageWithSignature {
    fn rlp_append(&self, s: &mut RlpStream) {
        self.encode_inner(s, false);
    }
}

impl Decodable for MessageWithSignature {
    fn decode(rlp: &Rlp) -> Result<Self, DecoderError> {
        let slice = rlp.data()?;

        let first = *slice.get(0).ok_or(DecoderError::Custom("empty slice"))?;

        if rlp.is_list() {
            if rlp.item_count()? != 9 {
                return Err(DecoderError::RlpIncorrectListLen);
            }

            let YParityAndChainId {
                odd_y_parity: odd,
                chain_id,
            } = YParityAndChainId::from_v(rlp.val_at(6)?)
                .ok_or(DecoderError::Custom("Invalid recovery ID"))?;
            let r = H256(rlp.val_at::<U256>(7)?.to_be_bytes());
            let s = H256(rlp.val_at::<U256>(8)?.to_be_bytes());
            let signature = MessageSignature::new(odd, r, s)
                .ok_or(DecoderError::Custom("Invalid transaction signature format"))?;

            return Ok(Self {
                message: Message::Legacy {
                    chain_id,
                    nonce: rlp.val_at(0)?,
                    gas_price: rlp.val_at(1)?,
                    gas_limit: rlp.val_at(2)?,
                    action: rlp.val_at(3)?,
                    value: rlp.val_at(4)?,
                    input: rlp.val_at::<Vec<u8>>(5)?.into(),
                },
                signature,
            });
        }

        let s = slice.get(1..).ok_or(DecoderError::Custom("no tx body"))?;

        if first == 0x01 {
            let rlp = Rlp::new(s);
            if rlp.item_count()? != 11 {
                return Err(DecoderError::RlpIncorrectListLen);
            }

            return Ok(Self {
                message: Message::EIP2930 {
                    chain_id: rlp.val_at(0)?,
                    nonce: rlp.val_at(1)?,
                    gas_price: rlp.val_at(2)?,
                    gas_limit: rlp.val_at(3)?,
                    action: rlp.val_at(4)?,
                    value: rlp.val_at(5)?,
                    input: rlp.val_at::<Vec<u8>>(6)?.into(),
                    access_list: rlp.list_at(7)?,
                },
                signature: MessageSignature::new(
                    rlp.val_at(8)?,
                    H256(rlp.val_at::<U256>(9)?.to_be_bytes()),
                    H256(rlp.val_at::<U256>(10)?.to_be_bytes()),
                )
                .ok_or(DecoderError::Custom("Invalid transaction signature format"))?,
            });
        }

        if first == 0x02 {
            let rlp = Rlp::new(s);
            if rlp.item_count()? != 12 {
                return Err(DecoderError::RlpIncorrectListLen);
            }

            return Ok(Self {
                message: Message::EIP1559 {
                    chain_id: rlp.val_at(0)?,
                    nonce: rlp.val_at(1)?,
                    max_priority_fee_per_gas: rlp.val_at(2)?,
                    max_fee_per_gas: rlp.val_at(3)?,
                    gas_limit: rlp.val_at(4)?,
                    action: rlp.val_at(5)?,
                    value: rlp.val_at(6)?,
                    input: rlp.val_at::<Vec<u8>>(7)?.into(),
                    access_list: rlp.list_at(8)?,
                },
                signature: MessageSignature::new(
                    rlp.val_at(9)?,
                    H256(rlp.val_at::<U256>(10)?.to_be_bytes()),
                    H256(rlp.val_at::<U256>(11)?.to_be_bytes()),
                )
                .ok_or(DecoderError::Custom("Invalid transaction signature format"))?,
            });
        }

        Err(DecoderError::Custom("invalid tx type"))
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

    pub(crate) fn priority_fee_per_gas(&self, base_fee_per_gas: U256) -> U256 {
        assert!(self.max_fee_per_gas() >= base_fee_per_gas);
        min(
            self.max_priority_fee_per_gas(),
            self.max_fee_per_gas() - base_fee_per_gas,
        )
    }

    pub(crate) fn effective_gas_price(&self, base_fee_per_gas: U256) -> U256 {
        self.priority_fee_per_gas(base_fee_per_gas) + base_fee_per_gas
    }
}

impl MessageWithSignature {
    pub fn hash(&self) -> H256 {
        H256::from_slice(Keccak256::digest(&self.trie_encode()).as_slice())
    }

    pub fn v(&self) -> u8 {
        self.signature.odd_y_parity as u8
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

        let rec = RecoveryId::from_i32(self.v() as i32)?;

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
        let bytes = hex!("f901e48080831000008080b90196608060405234801561001057600080fd5b50336000806101000a81548173ffffffffffffffffffffffffffffffffffffffff021916908373ffffffffffffffffffffffffffffffffffffffff1602179055507fc68045c3c562488255b55aa2c4c7849de001859ff0d8a36a75c2d5ed80100fb660405180806020018281038252600d8152602001807f48656c6c6f2c20776f726c64210000000000000000000000000000000000000081525060200191505060405180910390a160cf806100c76000396000f3fe6080604052348015600f57600080fd5b506004361060285760003560e01c80638da5cb5b14602d575b600080fd5b60336075565b604051808273ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200191505060405180910390f35b6000809054906101000a900473ffffffffffffffffffffffffffffffffffffffff168156fea265627a7a72315820fae816ad954005c42bea7bc7cb5b19f7fd5d3a250715ca2023275c9ca7ce644064736f6c634300050f003278a04cab43609092a99cf095d458b61b47189d1bbab64baed10a0fd7b7d2de2eb960a011ab1bcda76dfed5e733219beb83789f9887b2a7b2e61759c7c90f7d40403201");

        rlp::decode::<MessageWithSignature>(&bytes).unwrap();
    }

    #[test]
    fn transaction_legacy() {
        let tx = MessageWithSignature {
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

        assert_eq!(
            tx,
            rlp::decode::<MessageWithSignature>(&rlp::encode(&tx)).unwrap()
        );
    }

    #[test]
    fn transaction_eip2930() {
        let tx =
            MessageWithSignature {
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

        assert_eq!(
            tx,
            rlp::decode::<MessageWithSignature>(&rlp::encode(&tx)).unwrap()
        );
        assert_eq!(
            tx,
            MessageWithSignature::trie_decode(&tx.trie_encode()).unwrap()
        );
    }

    #[test]
    fn transaction_eip1559() {
        let tx =
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

        assert_eq!(
            tx,
            rlp::decode::<MessageWithSignature>(&rlp::encode(&tx)).unwrap()
        );
        assert_eq!(
            tx,
            MessageWithSignature::trie_decode(&tx.trie_encode()).unwrap()
        );
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
