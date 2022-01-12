use super::*;
use crate::crypto::*;
use derive_more::Deref;
use ethereum_types::*;
use parity_scale_codec::*;
use rlp_derive::*;
use sha3::*;
use std::borrow::Borrow;

#[derive(Clone, Debug, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub struct Block {
    pub header: BlockHeader,
    pub transactions: Vec<MessageWithSignature>,
    pub ommers: Vec<BlockHeader>,
}

impl Block {
    #[must_use]
    pub fn new(
        partial_header: PartialHeader,
        transactions: Vec<MessageWithSignature>,
        ommers: Vec<BlockHeader>,
    ) -> Self {
        let ommers_hash = Self::ommers_hash(&ommers);
        let transactions_root = Self::transactions_root(&transactions);

        Self {
            header: BlockHeader::new(partial_header, ommers_hash, transactions_root),
            transactions,
            ommers,
        }
    }

    pub fn ommers_hash(ommers: &[BlockHeader]) -> H256 {
        H256::from_slice(Keccak256::digest(&rlp::encode_list(ommers)[..]).as_slice())
    }

    pub fn transactions_root<I: IntoIterator<Item = T>, T: Borrow<MessageWithSignature>>(
        iter: I,
    ) -> H256 {
        ordered_trie_root(iter.into_iter().map(|r| r.borrow().trie_encode()))
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct BlockWithSenders {
    pub header: PartialHeader,
    pub transactions: Vec<MessageWithSender>,
    pub ommers: Vec<BlockHeader>,
}

impl From<Block> for BlockWithSenders {
    fn from(block: Block) -> Self {
        let transactions = block
            .transactions
            .into_iter()
            .map(|tx| {
                let sender = tx.recover_sender().unwrap();

                MessageWithSender {
                    message: tx.message,
                    sender,
                }
            })
            .collect();

        Self {
            header: block.header.into(),
            transactions,
            ommers: block.ommers,
        }
    }
}

#[derive(Clone, Debug, PartialEq, RlpEncodable, RlpDecodable)]
pub struct BlockBody {
    pub transactions: Vec<MessageWithSignature>,
    pub ommers: Vec<BlockHeader>,
}

impl From<Block> for BlockBody {
    fn from(block: Block) -> Self {
        Self {
            transactions: block.transactions,
            ommers: block.ommers,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct BlockBodyWithSenders {
    pub transactions: Vec<MessageWithSender>,
    pub ommers: Vec<BlockHeader>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Encode, Decode, RlpDecodable)]
pub struct BodyForStorage {
    pub base_tx_id: TxIndex,
    pub tx_amount: u64,
    pub uncles: Vec<BlockHeader>,
}

#[derive(Clone, Debug, Deref, Default)]
pub struct WithHash<T> {
    #[deref]
    pub inner: T,
    pub hash: H256,
}

#[cfg(test)]
mod tests {
    use super::*;

    const CHAIN_ID: ChainId = ChainId(1);

    #[test]
    fn compose_block() {
        // https://etherscan.io/block/13143465

        let partial_header = PartialHeader {
            parent_hash: hex!("51faecdaf8aac5c78b1cec1688cfb818a6bf9c6cd98c1240a713dea17e95b07d")
                .into(),
            beneficiary: hex!("5a0b54d5dc17e0aadc383d2db43b0a0d3e029c4c").into(),
            state_root: hex!("015ed1b1192150ee2ed92b3cd884e06bbbabf0128fe57e94c8d1fb1bc1c0989b")
                .into(),
            receipts_root: hex!("80ddaaa3e5058504b6fa033f40488a397e2263daa91e9f169e94937e760f6194")
                .into(),
            logs_bloom: hex!("40200002010000000000000080100010040a800000000000000100000400000000080808000020000000080004000840020000000a00210000800000002000000000000000010008080020080000402000000000804000000040a0200000000000002a0002200000000010002200090000000200000004000008005000080000004000000000000000400020000000000000080081020008000000400000000002000200000820000000000080002008080000000000000000200204000800004000000200000000000800008000000000000800020100100000000200002000001060000802000000201000002000c000480000400000000090000000000000").into(),
            difficulty: 0x1df7112c84f264_u64.into(),
            number: 0xc88da9_u64.into(),
            gas_limit: 0x1c9c380_u64,
            gas_used: 0xf1fa7_u64,
            timestamp: 0x61303112_u64,
            extra_data: hex::decode("d883010a08846765746888676f312e31362e37856c696e7578").unwrap().into(),
            mix_hash: hex!("b26583e11ffc5d412b46d1ddb74e78c775fb54b049dc0cf0689e8430a45d9186").into(),
            nonce: hex!("596b98b5d0f8cc56").into(),
            base_fee_per_gas: Some(0x18aac2ec3d_u64.into()),
        };

        let ommers = vec![];

        let transactions = vec![
            MessageWithSignature {
                message: Message::EIP1559 {
                    chain_id: CHAIN_ID,
                    nonce: 20369,
                    max_priority_fee_per_gas: 0x50a3d0b5d_u64.into(),
                    max_fee_per_gas: 0x23a9e38cf8_u64.into(),
                    gas_limit: 1_200_000,
                    action: TransactionAction::Call(hex!("a57bd00134b2850b2a1c55860c9e9ea100fdd6cf").into()),
                    value: U256::zero(),
                    input: hex!("1cff79cd000000000000000000000000aa2ec16d77cfc057fb9c516282fef9da9de1e987000000000000000000000000000000000000000000000000000000000000004000000000000000000000000000000000000000000000000000000000000001844f0c7c0a00000000000000000000000000000000000000000000000000000000000001f4000000000000000000000000c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2000000000000000000000000a0b86991c6218b36c1d19d4a2e9eb0ce3606eb4800000000000000000000000056178a0d5f301baf6cf3e1cd53d9863437345bf9000000000000000000000000000000000000000000000000002386f26fc100000000000000000000000000000000000000000000000000a2a15d09519be00000000000000000000000000000000000000000000000daadf45a4bb347757560000000000000000000000000000000000000000000000000003453af3f6dd960000000000000000000000000000000000000003f994c7f39b6af041a3c553270000000000000000000000000000000000000000000000000000de0b6b3a76400000000000000000000000000000000000000000000000000000000000061303192000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000").to_vec().into(),
                    access_list: vec![],
                },
                signature: MessageSignature::new(false, hex!("9a8548ba3759730fe25be0412c0b183ec975d15da2f12653d5f0a2016ca01f27"), hex!("6d93f2176bfda918c06365e507c6c66a16d30b9e76d2d8e5a7f2802e3bcc6593")).unwrap()
            },
            MessageWithSignature {
                message: Message::EIP1559 {
                    chain_id: CHAIN_ID,
                    nonce: 318_955,
                    max_priority_fee_per_gas: 0x156ba0980_u64.into(),
                    max_fee_per_gas: 0x29f7bcba80_u64.into(),
                    gas_limit: 320_000,
                    action: TransactionAction::Call(hex!("0000006daea1723962647b7e189d311d757fb793").into()),
                    value: U256::zero(),
                    input: hex!("178979ae0000000000000000000000000000000476fde29330084b2b0b08a9f7d2ac6f2b0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000006000000000000000000000000000000000000000000000000000000000000001048803dbee00000000000000000000000000000000000000000000003411811118647e0000000000000000000000000000000000000000000000000000000000037d69868500000000000000000000000000000000000000000000000000000000000000a00000000000000000000000000000006daea1723962647b7e189d311d757fb79300000000000000000000000000000000000000000000000000000000613031680000000000000000000000000000000000000000000000000000000000000002000000000000000000000000a0b86991c6218b36c1d19d4a2e9eb0ce3606eb4800000000000000000000000031c8eacbffdd875c74b94b077895bd78cf1e64a300000000000000000000000000000000000000000000000000000000").to_vec().into(),
                    access_list: vec![],
                },
                signature: MessageSignature::new(true, hex!("9f56a8c52a7e8e37ecd8c8bff54a414a92d349ea72a5389b1f3ed0f86c3248be"), hex!("7aa4b9e5ff16553ea28fb8f2701a56c2c60e69810377ebbaabadddcae0677168")).unwrap()
            },
            MessageWithSignature {
                message: Message::EIP1559 {
                    chain_id: CHAIN_ID,
                    nonce: 0x4ddec,
                    max_priority_fee_per_gas: 0x156ba0980_u64.into(),
                    max_fee_per_gas: 0x29f7bcba80_u64.into(),
                    gas_limit: 0x4e200,
                    action: TransactionAction::Call(hex!("0000006daea1723962647b7e189d311d757fb793").into()),
                    value: U256::zero(),
                    input: hex!("178979ae0000000000000000000000000000005c9426e6910f22f0c00ed3690a4884dd6e00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000060000000000000000000000000000000000000000000000000000000000000010438ed173900000000000000000000000000000000000000000000023bb2f4021291c00000000000000000000000000000000000000000000000000000000000038277eafc00000000000000000000000000000000000000000000000000000000000000a00000000000000000000000000000006daea1723962647b7e189d311d757fb79300000000000000000000000000000000000000000000000000000000613031860000000000000000000000000000000000000000000000000000000000000002000000000000000000000000d417144312dbf50465b1c641d016962017ef6240000000000000000000000000a0b86991c6218b36c1d19d4a2e9eb0ce3606eb4800000000000000000000000000000000000000000000000000000000").to_vec().into(),
                    access_list: vec![],
                },
                signature: MessageSignature::new(true, hex!("e0af864ce72e3755ef3e1c0eb8e665300f9374fc0856ebf54340bcf8daddfdf4"), hex!("488a890a71fb95db2088e8c261149b1ca33902b039eb461805261cd2180b38bb")).unwrap(),
            },
            MessageWithSignature {
                message: Message::EIP1559 {
                    chain_id: CHAIN_ID,
                    nonce: 0x3f8,
                    max_priority_fee_per_gas: 0x77359400_u64.into(),
                    max_fee_per_gas: 0x20f823e84c_u64.into(),
                    gas_limit: 0x2cfad,
                    action: TransactionAction::Call(hex!("e592427a0aece92de3edee1f18e0157c05861564").into()),
                    value: U256::zero(),
                    input: hex!("ac9650d800000000000000000000000000000000000000000000000000000000000000200000000000000000000000000000000000000000000000000000000000000002000000000000000000000000000000000000000000000000000000000000004000000000000000000000000000000000000000000000000000000000000001800000000000000000000000000000000000000000000000000000000000000104414bf389000000000000000000000000515d7e9d75e2b76db60f8a051cd890eba23286bc000000000000000000000000c02aaa39b223fe8d0a0e5c4f27ead9083c756cc20000000000000000000000000000000000000000000000000000000000000bb80000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000006130358000000000000000000000000000000000000000000000001043561a882930000000000000000000000000000000000000000000000000000001addc207d623fcc000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000004449404b7c00000000000000000000000000000000000000000000000001addc207d623fcc000000000000000000000000ac569e0f62c7cbbb987518692aae056a0ae1dd1800000000000000000000000000000000000000000000000000000000").to_vec().into(),
                    access_list: vec![],
                },
                signature: MessageSignature::new(false, hex!("22c1771e804cd2da132d19b10c558847860f6a61e6ca6f1eec54ee747d374055"), hex!("41a2f6fcd021e2977d5d555ed1413632638a0bf1b7e86b8c46ab9dd77effdb71")).unwrap(),
            },
            MessageWithSignature {
                message: Message::EIP1559 {
                    chain_id: CHAIN_ID,
                    nonce: 0x5a9f9,
                    max_priority_fee_per_gas: 0x77359400_u64.into(),
                    max_fee_per_gas: 0x293605aa00_u64.into(),
                    gas_limit: 0x3d090,
                    action: TransactionAction::Call(hex!("a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48").into()),
                    value: U256::zero(),
                    input: hex!("a9059cbb0000000000000000000000005c874f13a92f5c35aec3d7bc07c630a92a79289a000000000000000000000000000000000000000000000000000000003b9aca00").to_vec().into(),
                    access_list: vec![],
                },
                signature: MessageSignature::new(true, hex!("6dd51acdc109fcbe29ebf526c4e93cef449dad611e0123cfa221770d1619aa55"), hex!("7112d13e643b2288166ae49770b7df778f749f45f28c1f33c8d6914b5f65a4f2")).unwrap(),
            },
            MessageWithSignature {
                message: Message::EIP1559 {
                    chain_id: CHAIN_ID,
                    nonce: 0x36d,
                    max_priority_fee_per_gas: 0x73a20d00_u64.into(),
                    max_fee_per_gas: 0x226f4988d9_u64.into(),
                    gas_limit: 0xd9c5,
                    action: TransactionAction::Call(hex!("e66b3aa360bb78468c00bebe163630269db3324f").into()),
                    value: U256::zero(),
                    input: hex!("095ea7b30000000000000000000000007a250d5630b4cf539739df2c5dacb4c659f2488dffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff").to_vec().into(),
                    access_list: vec![],
                },
                signature: MessageSignature::new(false, hex!("629b9b5baed83904eed041b38d3debc572cc7ad4be55aac679a470e77d1152de"), hex!("29eacf16c562d6ffd996cef9f96bb03b58ad775e28b68831f0e8fbeec42d60bd")).unwrap(),
            },
            MessageWithSignature {
                message: Message::EIP1559 {
                    chain_id: CHAIN_ID,
                    nonce: 0x23,
                    max_priority_fee_per_gas: 0x3b9aca00_u64.into(),
                    max_fee_per_gas: 0x28dcc4b35e_u64.into(),
                    gas_limit: 0x63006,
                    action: TransactionAction::Call(hex!("ca414feacd26006c3748a187bedf455bef5fc57d").into()),
                    value: 0x11c37937e080000_u64.into(),
                    input: hex!("a0712d680000000000000000000000000000000000000000000000000000000000000002").to_vec().into(),
                    access_list: vec![],
                },
                signature: MessageSignature::new(false, hex!("70bcb39ac6f540498c3adfdf3a23ecce5cf7b4f75b0674c157da02350edf8ed4"), hex!("40e997c09def486888c34e77565cce82b348d0035e2ea36bf125252f7895ff3c")).unwrap(),
            },
        ];

        assert_eq!(
            transactions
                .iter()
                .map(|tx| hex::encode(tx.hash().0))
                .collect::<Vec<_>>(),
            vec![
                "7a903d768be1c9b1399e792e189cb468a55002d654739f830e45782d4dfae690",
                "25f3b3a81e87b5d3a745711f57c38216f5c82cc32c04f531cfe4db25fd074cef",
                "2f7e1a1af4b7f3e13ddbcc333fec82e8b640b3a0815f3cb2fc52354cdb3e8762",
                "93de2b0b54dcfef2285d7c116b7a14f9f986da6515ec5becdce7b521551caf9f",
                "78cc131432f7224660f6ab9c3e9817e7e7213ea06002b53b26bacd0f92a83c0a",
                "e6554f7d6419e3b6f70e956abeb02d524557b9f81cf883178fa2e5ef2d8b7139",
                "d17aaa0f3bf37d0535fb4d0942e48a697eeb2bb5399aa300e1ef1f588c9dddc7"
            ]
        );

        let block = Block::new(partial_header, transactions, ommers);

        assert_eq!(
            block.header.transactions_root.0,
            hex!("6aaee4a301af3f721f01f886c50db6ff354487e0a3b713601797a439475ade0c")
        );
        assert_eq!(
            block.header.ommers_hash.0,
            hex!("1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347")
        );
        assert_eq!(
            block.header.hash().0,
            hex!("05c0d29761f97e4bf5c6b64e9fef4a7f8a884483de8c379ff00847d559ba361b")
        );
    }

    #[test]
    fn block_body_rlp() {
        // https://etherscan.io/block/3
        let rlp_hex = hex!(
            "f90219c0f90215f90212a0d4e56740f876aef8c010b86a40d5f56745a118d090"
            "6a34e69aec8c0db1cb8fa3a01dcc4de8dec75d7aab85b567b6ccd41ad312451b"
            "948a7413f0a142fd40d4934794c8ebccc5f5689fa8659d83713341e5ad193494"
            "48a01e6e030581fd1873b4784280859cd3b3c04aa85520f08c304cf5ee63d393"
            "5adda056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e3"
            "63b421a056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5"
            "e363b421b9010000000000000000000000000000000000000000000000000000"
            "0000000000000000000000000000000000000000000000000000000000000000"
            "0000000000000000000000000000000000000000000000000000000000000000"
            "0000000000000000000000000000000000000000000000000000000000000000"
            "0000000000000000000000000000000000000000000000000000000000000000"
            "0000000000000000000000000000000000000000000000000000000000000000"
            "0000000000000000000000000000000000000000000000000000000000000000"
            "0000000000000000000000000000000000000000000000000000000000000000"
            "000000000000008503ff80000001821388808455ba42429a5961746573205261"
            "6e64616c6c202d2045746865724e696e6a61a0f8c94dfe61cf26dcdf8cffeda3"
            "37cf6a903d65c449d7691a022837f6e2d994598868b769c5451a7aea"
        )
        .to_vec();

        let bb = rlp::decode::<BlockBody>(&rlp_hex).unwrap();

        assert_eq!(bb.transactions, []);
        assert_eq!(
            bb.ommers,
            vec![BlockHeader {
                parent_hash: hex!(
                    "d4e56740f876aef8c010b86a40d5f56745a118d0906a34e69aec8c0db1cb8fa3"
                )
                .into(),
                ommers_hash: EMPTY_LIST_HASH,
                beneficiary: hex!("c8ebccc5f5689fa8659d83713341e5ad19349448").into(),
                state_root: hex!(
                    "1e6e030581fd1873b4784280859cd3b3c04aa85520f08c304cf5ee63d3935add"
                )
                .into(),
                transactions_root: EMPTY_ROOT,
                receipts_root: EMPTY_ROOT,
                logs_bloom: Bloom::zero(),
                difficulty: 17_171_480_576_u64.into(),
                number: 1.into(),
                gas_limit: 5000,
                gas_used: 0,
                timestamp: 1438270018,
                extra_data: b"Yates Randall - EtherNinja".to_vec().into(),
                mix_hash: hex!("f8c94dfe61cf26dcdf8cffeda337cf6a903d65c449d7691a022837f6e2d99459")
                    .into(),
                nonce: hex!("68b769c5451a7aea").into(),
                base_fee_per_gas: None,
            }]
        );

        assert_eq!(rlp::encode(&bb), rlp_hex);
    }

    #[test]
    fn block_body_rlp_2() {
        let body = BlockBody {
            transactions: vec![
                MessageWithSignature {
                    message: Message::Legacy {
                        chain_id: None,
                        nonce: 172339,
                        gas_price: U256::from(50 * GIGA),
                        gas_limit: 90_000,
                        action: TransactionAction::Call(
                            hex!("e5ef458d37212a06e3f59d40c454e76150ae7c32").into(),
                        ),
                        value: U256::from(1_027_501_080_u128 * u128::from(GIGA)),
                        input: vec![].into(),
                    },
                    signature: MessageSignature::new(
                        false,
                        hex!("48b55bfa915ac795c431978d8a6a992b628d557da5ff759b307d495a36649353"),
                        hex!("1fffd310ac743f371de3b9f7f9cb56c0b28ad43601b4ab949f53faa07bd2c804"),
                    )
                    .unwrap(),
                },
                MessageWithSignature {
                    message: Message::EIP1559 {
                        chain_id: CHAIN_ID,
                        nonce: 1,
                        max_priority_fee_per_gas: U256::from(5 * GIGA),
                        max_fee_per_gas: U256::from(30 * GIGA),
                        gas_limit: 1_000_000,
                        action: TransactionAction::Create,
                        value: 0.into(),
                        input: hex!("602a6000556101c960015560068060166000396000f3600035600055")
                            .to_vec()
                            .into(),
                        access_list: vec![],
                    },
                    signature: MessageSignature::new(
                        false,
                        hex!("52f8f61201b2b11a78d6e866abc9c3db2ae8631fa656bfe5cb53668255367afb"),
                        hex!("52f8f61201b2b11a78d6e866abc9c3db2ae8631fa656bfe5cb53668255367afb"),
                    )
                    .unwrap(),
                },
            ],
            ommers: vec![BlockHeader {
                parent_hash: hex!(
                    "b397a22bb95bf14753ec174f02f99df3f0bdf70d1851cdff813ebf745f5aeb55"
                )
                .into(),
                ommers_hash: EMPTY_LIST_HASH,
                beneficiary: hex!("0c729be7c39543c3d549282a40395299d987cec2").into(),
                state_root: hex!(
                    "c2bcdfd012534fa0b19ffba5fae6fc81edd390e9b7d5007d1e92e8e835286e9d"
                )
                .into(),
                transactions_root: EMPTY_ROOT,
                receipts_root: EMPTY_ROOT,
                logs_bloom: Bloom::zero(),
                difficulty: 12_555_442_155_599_u128.into(),
                number: 13_000_013.into(),
                gas_limit: 3_141_592,
                gas_used: 0,
                timestamp: 1455404305,
                extra_data: vec![].into(),
                mix_hash: hex!("f0a53dfdd6c2f2a661e718ef29092de60d81d45f84044bec7bf4b36630b2bc08")
                    .into(),
                nonce: hex!("0000000000000023").into(),
                base_fee_per_gas: None,
            }],
        };

        assert_eq!(rlp::decode::<BlockBody>(&rlp::encode(&body)).unwrap(), body);
    }

    #[test]
    fn invalid_block_rlp() {
        // Consensus test RLP_InputList_TooManyElements_HEADER_DECODEINTO_BLOCK_EXTBLOCK_HEADER
        let rlp_hex = hex!(
            "f90260f90207a068a61c4a05db4913009de5666753258eb9306157680dc5da0d93656550c9257ea01dcc4de8dec75d7aab85b567b6cc"
            "d41ad312451b948a7413f0a142fd40d49347948888f1f195afa192cfee860698584c030f4c9db1a0ef1552a40b7165c3cd773806b9e0c1"
            "65b75356e0314bf0706f279c729f51e017a0b6c9fd1447d0b414a1f05957927746f58ef5a2ebde17db631d460eaf6a93b18da0bc37d797"
            "53ad738a6dac4921e57392f145d8887476de3f783dfa7edae9283e52b90100000000000000000000000000000000000000000000000000"
            "00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
            "00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
            "00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
            "00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
            "0000000000000000000000008302000001832fefd8825208845509814280a00451dd53d9c09f3cfb627b51d9d80632ed801f6330ee584b"
            "ffc26caac9b9249f88c7bffe5ebd94cc2ff861f85f800a82c35094095e7baea6a6c7c4c2dfeb977efac326af552d870a801ba098c3a099"
            "885a281885f487fd37550de16436e8c47874cd213531b10fe751617fa044b6b81011ce57bffcaf610bf728fb8a7237ad261ea2d937423d"
            "78eb9e137076c0").to_vec();

        assert!(rlp::decode::<Block>(&rlp_hex).is_err())
    }

    #[test]
    fn eip2718_block_rlp() {
        let rlp_hex = hex!(
            "f90319f90211a00000000000000000000000000000000000000000000000000000000000000000a01dcc4de8dec75d7aab85b567b6ccd4"
            "1ad312451b948a7413f0a142fd40d49347948888f1f195afa192cfee860698584c030f4c9db1a0ef1552a40b7165c3cd773806b9e0c165"
            "b75356e0314bf0706f279c729f51e017a0e6e49996c7ec59f7a23d22b83239a60151512c65613bf84a0d7da336399ebc4aa0cafe75574d"
            "59780665a97fbfd11365c7545aa8f1abf4e5e12e8243334ef7286bb9010000000000000000000000000000000000000000000000000000"
            "00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
            "00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
            "00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
            "00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
            "000000000000000000000083020000820200832fefd882a410845506eb0796636f6f6c65737420626c6f636b206f6e20636861696ea0bd"
            "4472abb6659ebe3ee06ee4d7b72a00a9f4d001caca51342001075469aff49888a13a5a8c8f2bb1c4f90101f85f800a82c35094095e7bae"
            "a6a6c7c4c2dfeb977efac326af552d870a801ba09bea4c4daac7c7c52e093e6a4c35dbbcf8856f1af7b059ba20253e70848d094fa08a8f"
            "ae537ce25ed8cb5af9adac3f141af69bd515bd2ba031522df09b97dd72b1b89e01f89b01800a8301e24194095e7baea6a6c7c4c2dfeb97"
            "7efac326af552d878080f838f7940000000000000000000000000000000000000001e1a000000000000000000000000000000000000000"
            "0000000000000000000000000001a03dbacc8d0259f2508625e97fdfc57cd85fdd16e5821bc2c10bdd1a52649e8335a0476e10695b183a"
            "87b0aa292a7f4b78ef0c3fbe62aa2c42c84e1d9c3da159ef14c0").to_vec();

        let block = rlp::decode::<Block>(&rlp_hex).unwrap();

        assert_eq!(block.transactions.len(), 2);

        assert_eq!(block.transactions[0].tx_type(), TxType::Legacy);
        assert_eq!(block.transactions[0].access_list().into_owned(), vec![]);

        assert_eq!(block.transactions[1].tx_type(), TxType::EIP2930);
        assert_eq!(block.transactions[1].access_list().len(), 1);
    }

    #[test]
    fn eip1559_header_rlp() {
        let h = BlockHeader {
            number: 13_500_000.into(),
            base_fee_per_gas: Some(2_700_000_000_u64.into()),
            ..BlockHeader::empty()
        };

        assert_eq!(rlp::decode::<BlockHeader>(&rlp::encode(&h)).unwrap(), h);
    }
}
