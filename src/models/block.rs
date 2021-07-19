use super::*;
use crate::crypto::*;
use ethereum_types::*;
use rlp_derive::*;
use sha3::*;

#[derive(Clone, Debug, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub struct Block {
    pub header: BlockHeader,
    pub transactions: Vec<Transaction>,
    pub ommers: Vec<BlockHeader>,
}

impl Block {
    #[must_use]
    pub fn new(
        partial_header: PartialHeader,
        transactions: Vec<Transaction>,
        ommers: Vec<BlockHeader>,
    ) -> Self {
        let ommers_hash =
            H256::from_slice(Keccak256::digest(&rlp::encode_list(&ommers)[..]).as_slice());
        let transactions_root =
            ordered_trie_root(transactions.iter().map(|r| rlp::encode(r).freeze()));

        Self {
            header: BlockHeader::new(partial_header, ommers_hash, transactions_root),
            transactions,
            ommers,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct BlockBody {
    pub transactions: Vec<Transaction>,
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
    pub transactions: Vec<TransactionWithSender>,
    pub ommers: Vec<BlockHeader>,
}

#[derive(RlpDecodable, RlpEncodable, Debug, PartialEq)]
pub struct BodyForStorage {
    pub base_tx_id: u64,
    pub tx_amount: u32,
    pub uncles: Vec<BlockHeader>,
}
