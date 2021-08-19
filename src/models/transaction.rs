use bytes::Bytes;
use ethereum_types::*;
use rlp::{Decodable, DecoderError, Encodable, Rlp, RlpStream};
use secp256k1::{
    recovery::{RecoverableSignature, RecoveryId},
    Message, SECP256K1,
};
use sha3::*;
use static_bytes::{BufMut, BytesMut};
use std::{borrow::Cow, cmp::min, ops::Deref};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum TxType {
    Legacy = 0,
    EIP2930 = 1,
    EIP1559 = 2,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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
    pub chain_id: Option<u64>,
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
            let chain_id = Some(w >> 1); // w / 2
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
            chain_id * 2 + 35 + self.odd_y_parity as u64
        } else {
            27 + self.odd_y_parity as u64
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransactionSignature {
    odd_y_parity: bool,
    r: H256,
    s: H256,
}

impl TransactionSignature {
    #[must_use]
    pub fn new(odd_y_parity: bool, r: H256, s: H256) -> Option<Self> {
        const LOWER: H256 = H256([
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x01,
        ]);
        const UPPER: H256 = H256([
            0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
            0xff, 0xfe, 0xba, 0xae, 0xdc, 0xe6, 0xaf, 0x48, 0xa0, 0x3b, 0xbf, 0xd2, 0x5e, 0x8c,
            0xd0, 0x36, 0x41, 0x41,
        ]);

        let is_valid = r < UPPER && r >= LOWER && s < UPPER && s >= LOWER;

        if is_valid {
            Some(Self { odd_y_parity, r, s })
        } else {
            None
        }
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

#[derive(Clone, Debug, PartialEq, Eq)]
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TransactionMessage {
    Legacy {
        chain_id: Option<u64>,
        nonce: u64,
        gas_price: U256,
        gas_limit: u64,
        action: TransactionAction,
        value: U256,
        input: Bytes<'static>,
    },
    EIP2930 {
        chain_id: u64,
        nonce: u64,
        gas_price: U256,
        gas_limit: u64,
        action: TransactionAction,
        value: U256,
        input: Bytes<'static>,
        access_list: Vec<AccessListItem>,
    },
    EIP1559 {
        chain_id: u64,
        nonce: u64,
        max_priority_fee_per_gas: U256,
        max_fee_per_gas: U256,
        gas_limit: u64,
        action: TransactionAction,
        value: U256,
        input: Bytes<'static>,
        access_list: Vec<AccessListItem>,
    },
}

impl TransactionMessage {
    pub fn hash(&self) -> H256 {
        H256::from_slice(Keccak256::digest(&rlp::encode(self)).as_slice())
    }
}

impl Encodable for TransactionMessage {
    fn rlp_append(&self, s: &mut RlpStream) {
        match self {
            TransactionMessage::Legacy {
                chain_id,
                nonce,
                gas_price,
                gas_limit,
                action,
                value,
                input,
            } => {
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
            }
            TransactionMessage::EIP2930 {
                chain_id,
                nonce,
                gas_price,
                gas_limit,
                action,
                value,
                input,
                access_list,
            } => {
                s.begin_list(8);
                s.append(chain_id);
                s.append(nonce);
                s.append(gas_price);
                s.append(gas_limit);
                s.append(action);
                s.append(value);
                s.append(&input.as_ref());
                s.append_list(access_list);
            }
            TransactionMessage::EIP1559 {
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
                s.begin_list(8);
                s.append(chain_id);
                s.append(nonce);
                s.append(max_priority_fee_per_gas);
                s.append(max_fee_per_gas);
                s.append(gas_limit);
                s.append(action);
                s.append(value);
                s.append(&input.as_ref());
                s.append_list(access_list);
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EIP2930TransactionMessage {
    pub chain_id: u64,
    pub nonce: u64,
    pub gas_price: U256,
    pub gas_limit: u64,
    pub action: TransactionAction,
    pub value: U256,
    pub input: Bytes<'static>,
    pub access_list: Vec<AccessListItem>,
}

impl Encodable for EIP2930TransactionMessage {
    fn rlp_append(&self, s: &mut RlpStream) {
        s.begin_list(8);
        s.append(&self.chain_id);
        s.append(&self.nonce);
        s.append(&self.gas_price);
        s.append(&self.gas_limit);
        s.append(&self.action);
        s.append(&self.value);
        s.append(&self.input.as_ref());
        s.append_list(&self.access_list);
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Transaction {
    pub message: TransactionMessage,
    pub signature: TransactionSignature,
}

impl Deref for Transaction {
    type Target = TransactionMessage;

    fn deref(&self) -> &Self::Target {
        &self.message
    }
}

impl Encodable for Transaction {
    fn rlp_append(&self, s: &mut RlpStream) {
        match &self.message {
            TransactionMessage::Legacy {
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
                s.append(&U256::from_big_endian(&self.signature.r[..]));
                s.append(&U256::from_big_endian(&self.signature.s[..]));
            }
            TransactionMessage::EIP2930 {
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
                s1.append(&U256::from_big_endian(&self.signature.r[..]));
                s1.append(&U256::from_big_endian(&self.signature.s[..]));
                s1.out().rlp_append(s)
            }
            TransactionMessage::EIP1559 {
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
                s1.append(&U256::from_big_endian(&self.signature.r[..]));
                s1.append(&U256::from_big_endian(&self.signature.s[..]));
                s1.out().rlp_append(s)
            }
        }
    }
}

impl Decodable for Transaction {
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
            let r = {
                let mut rarr = [0_u8; 32];
                rlp.val_at::<U256>(7)?.to_big_endian(&mut rarr);
                H256::from(rarr)
            };
            let s = {
                let mut sarr = [0_u8; 32];
                rlp.val_at::<U256>(8)?.to_big_endian(&mut sarr);
                H256::from(sarr)
            };
            let signature = TransactionSignature::new(odd, r, s)
                .ok_or(DecoderError::Custom("Invalid transaction signature format"))?;

            return Ok(Self {
                message: TransactionMessage::Legacy {
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
                message: TransactionMessage::EIP2930 {
                    chain_id: rlp.val_at(0)?,
                    nonce: rlp.val_at(1)?,
                    gas_price: rlp.val_at(2)?,
                    gas_limit: rlp.val_at(3)?,
                    action: rlp.val_at(4)?,
                    value: rlp.val_at(5)?,
                    input: rlp.val_at::<Vec<u8>>(6)?.into(),
                    access_list: rlp.list_at(7)?,
                },
                signature: TransactionSignature::new(
                    rlp.val_at(8)?,
                    {
                        let mut rarr = [0_u8; 32];
                        rlp.val_at::<U256>(9)?.to_big_endian(&mut rarr);
                        H256::from(rarr)
                    },
                    {
                        let mut sarr = [0_u8; 32];
                        rlp.val_at::<U256>(10)?.to_big_endian(&mut sarr);
                        H256::from(sarr)
                    },
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
                message: TransactionMessage::EIP1559 {
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
                signature: TransactionSignature::new(
                    rlp.val_at(9)?,
                    {
                        let mut rarr = [0_u8; 32];
                        rlp.val_at::<U256>(10)?.to_big_endian(&mut rarr);
                        H256::from(rarr)
                    },
                    {
                        let mut sarr = [0_u8; 32];
                        rlp.val_at::<U256>(11)?.to_big_endian(&mut sarr);
                        H256::from(sarr)
                    },
                )
                .ok_or(DecoderError::Custom("Invalid transaction signature format"))?,
            });
        }

        Err(DecoderError::Custom("invalid tx type"))
    }
}

impl TransactionMessage {
    pub const fn tx_type(&self) -> TxType {
        match self {
            Self::Legacy { .. } => TxType::Legacy,
            Self::EIP2930 { .. } => TxType::EIP2930,
            Self::EIP1559 { .. } => TxType::EIP1559,
        }
    }

    pub fn chain_id(&self) -> Option<u64> {
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

    pub const fn input(&self) -> &Bytes<'static> {
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
}

impl Transaction {
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

        let public = &SECP256K1.recover(
            &Message::from_slice(self.message.hash().as_bytes())?,
            &RecoverableSignature::from_compact(&sig, rec)?,
        )?;

        let address_slice = &Keccak256::digest(&public.serialize_uncompressed()[1..])[12..];
        Ok(Address::from_slice(address_slice))
    }
}

#[derive(Clone, Debug)]
pub struct TransactionWithSender {
    pub tx_type: TxType,
    pub chain_id: Option<u64>,
    pub nonce: u64,
    pub max_priority_fee_per_gas: U256,
    pub max_fee_per_gas: U256,
    pub gas_limit: u64,
    pub action: TransactionAction,
    pub value: U256,
    pub input: Bytes<'static>,
    pub access_list: AccessList,
    pub sender: Address,
}

impl TransactionWithSender {
    pub fn new(msg: &TransactionMessage, sender: Address) -> Self {
        Self {
            tx_type: msg.tx_type(),
            chain_id: msg.chain_id(),
            nonce: msg.nonce(),
            max_priority_fee_per_gas: msg.max_priority_fee_per_gas(),
            max_fee_per_gas: msg.max_fee_per_gas(),
            gas_limit: msg.gas_limit(),
            action: msg.action(),
            value: msg.value(),
            input: msg.input().clone(),
            access_list: msg.access_list().into_owned(),
            sender,
        }
    }

    pub(crate) fn priority_fee_per_gas(&self, base_fee_per_gas: U256) -> U256 {
        assert!(self.max_fee_per_gas >= base_fee_per_gas);
        min(
            self.max_priority_fee_per_gas,
            self.max_fee_per_gas - base_fee_per_gas,
        )
    }

    pub(crate) fn effective_gas_price(&self, base_fee_per_gas: U256) -> U256 {
        self.priority_fee_per_gas(base_fee_per_gas) + base_fee_per_gas
    }

    #[cfg(test)]
    pub(crate) fn empty() -> Self {
        Self {
            tx_type: TxType::Legacy,
            sender: Address::zero(),
            action: TransactionAction::Create,
            value: U256::zero(),
            chain_id: None,
            nonce: 0,
            max_priority_fee_per_gas: U256::zero(),
            max_fee_per_gas: U256::zero(),
            gas_limit: 0,
            access_list: Default::default(),
            input: Bytes::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hex_literal::hex;

    #[test]
    fn can_decode_raw_transaction() {
        let bytes = hex!("f901e48080831000008080b90196608060405234801561001057600080fd5b50336000806101000a81548173ffffffffffffffffffffffffffffffffffffffff021916908373ffffffffffffffffffffffffffffffffffffffff1602179055507fc68045c3c562488255b55aa2c4c7849de001859ff0d8a36a75c2d5ed80100fb660405180806020018281038252600d8152602001807f48656c6c6f2c20776f726c64210000000000000000000000000000000000000081525060200191505060405180910390a160cf806100c76000396000f3fe6080604052348015600f57600080fd5b506004361060285760003560e01c80638da5cb5b14602d575b600080fd5b60336075565b604051808273ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200191505060405180910390f35b6000809054906101000a900473ffffffffffffffffffffffffffffffffffffffff168156fea265627a7a72315820fae816ad954005c42bea7bc7cb5b19f7fd5d3a250715ca2023275c9ca7ce644064736f6c634300050f003278a04cab43609092a99cf095d458b61b47189d1bbab64baed10a0fd7b7d2de2eb960a011ab1bcda76dfed5e733219beb83789f9887b2a7b2e61759c7c90f7d40403201");

        rlp::decode::<Transaction>(&bytes).unwrap();
    }

    #[test]
    fn transaction_legacy() {
        let tx = Transaction {
            message: TransactionMessage::Legacy {
                chain_id: Some(2),
                nonce: 12,
                gas_price: 20_000_000_000_u64.into(),
                gas_limit: 21000,
                action: TransactionAction::Call(
                    hex!("727fc6a68321b754475c668a6abfb6e9e71c169a").into(),
                ),
                value: U256::from(10) * 1_000_000_000 * 1_000_000_000,
                input: hex!("a9059cbb000000000213ed0f886efd100b67c7e4ec0a85a7d20dc971600000000000000000000015af1d78b58c4000").to_vec().into(),
            },
			signature: TransactionSignature::new(
                true,
                hex!("be67e0a07db67da8d446f76add590e54b6e92cb6b8f9835aeb67540579a27717").into(),
                hex!("2d690516512020171c1ec870f6ff45398cc8609250326be89915fb538e7bd718").into(),
            ).unwrap(),
		};

        assert_eq!(tx, rlp::decode::<Transaction>(&rlp::encode(&tx)).unwrap());
    }

    #[test]
    fn transaction_eip2930() {
        let tx =
            Transaction {
                message: TransactionMessage::EIP2930 {
                    chain_id: 5,
                    nonce: 7,
                    gas_price: 30_000_000_000_u64.into(),
                    gas_limit: 5_748_100_u64,
                    action: TransactionAction::Call(
                        hex!("811a752c8cd697e3cb27279c330ed1ada745a8d7").into(),
                    ),
                    value: U256::from(2) * 1_000_000_000 * 1_000_000_000,
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
                signature: TransactionSignature::new(
                    false,
                    hex!("36b241b061a36a32ab7fe86c7aa9eb592dd59018cd0443adc0903590c16b02b0").into(),
                    hex!("5edcc541b4741c5cc6dd347c5ed9577ef293a62787b4510465fadbfe39ee4094").into(),
                )
                .unwrap(),
            };

        assert_eq!(tx, rlp::decode::<Transaction>(&rlp::encode(&tx)).unwrap());
    }

    #[test]
    fn transaction_eip1559() {
        let tx =
            Transaction {
                message: TransactionMessage::EIP1559 {
                    chain_id: 5,
                    nonce: 7,
                    max_priority_fee_per_gas: 10_000_000_000_u64.into(),
                    max_fee_per_gas: 30_000_000_000_u64.into(),
                    gas_limit: 5_748_100_u64,
                    action: TransactionAction::Call(
                        hex!("811a752c8cd697e3cb27279c330ed1ada745a8d7").into(),
                    ),
                    value: U256::from(2) * 1_000_000_000 * 1_000_000_000,
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
                signature: TransactionSignature::new(
                    false,
                    hex!("36b241b061a36a32ab7fe86c7aa9eb592dd59018cd0443adc0903590c16b02b0").into(),
                    hex!("5edcc541b4741c5cc6dd347c5ed9577ef293a62787b4510465fadbfe39ee4094").into(),
                )
                .unwrap(),
            };

        assert_eq!(tx, rlp::decode::<Transaction>(&rlp::encode(&tx)).unwrap());
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
                chain_id: Some(0)
            }
        );
        assert_eq!(
            YParityAndChainId::from_v(36).unwrap(),
            YParityAndChainId {
                odd_y_parity: true,
                chain_id: Some(0)
            }
        );
        assert_eq!(
            YParityAndChainId::from_v(37).unwrap(),
            YParityAndChainId {
                odd_y_parity: false,
                chain_id: Some(1)
            }
        );
        assert_eq!(
            YParityAndChainId::from_v(38).unwrap(),
            YParityAndChainId {
                odd_y_parity: true,
                chain_id: Some(1)
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
                chain_id: Some(1)
            }
            .v(),
            37
        );
        assert_eq!(
            YParityAndChainId {
                odd_y_parity: true,
                chain_id: Some(1)
            }
            .v(),
            38
        );
    }
}
