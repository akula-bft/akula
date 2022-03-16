use super::{hash_builder::unpack_nibbles, HashBuilder};
use crate::models::*;
use bytes::{BufMut, BytesMut};
use fastrlp::Encodable;

pub trait TrieEncode {
    fn trie_encode(&self, out: &mut dyn BufMut);
}

/// Lexicographic order for RLP-encoded integers is the same as their natural order,
/// save for 0, which, due to its RLP encoding, should be placed between 0x7f and 0x80.
const fn adjust_index_for_rlp(i: usize, len: usize) -> usize {
    if i > 0x7f {
        i
    } else if i == 0x7f || i + 1 == len {
        0
    } else {
        i + 1
    }
}

/// Trie root hash of RLP-encoded values, the keys are RLP-encoded integers.
/// See Section 4.3.2. "Holistic Validity" of the Yellow Paper.
pub fn root_hash<T>(values: &[T]) -> H256
where
    T: TrieEncode,
{
    let mut index_rlp = BytesMut::new();
    let mut value_rlp = BytesMut::new();

    let mut hb = HashBuilder::<'static>::new(None);

    let iter_len = values.len();

    for j in 0..values.len() {
        let index = adjust_index_for_rlp(j, iter_len);
        index_rlp.clear();
        index.encode(&mut index_rlp);
        value_rlp.clear();
        values[index].trie_encode(&mut value_rlp);

        hb.add_leaf(unpack_nibbles(&index_rlp), &value_rlp);
    }

    hb.compute_root_hash()
}

#[cfg(test)]
mod tests {
    use super::*;
    use hex_literal::hex;

    #[test]
    fn empty_root_hash() {
        assert_eq!(root_hash::<MessageWithSignature>(&[]), EMPTY_ROOT)
    }

    #[test]
    fn hardcoded_root_hash() {
        assert_eq!(root_hash(&[
            (21_000, vec![]),
            (42_000, vec![]),
            (
                65_092,
                vec![Log {
                    address: hex!("8d12a197cb00d4747a1fe03395095ce2a5cc6819").into(),
                    topics: vec![hex!("f341246adaac6f497bc2a656f546ab9e182111d630394f0c57c710a59a2cb567").into()],
                    data: hex!("000000000000000000000000000000000000000000000000000000000000000000000000000000000000000043b2126e7a22e0c288dfb469e3de4d2c097f3ca0000000000000000000000000000000000000000000000001195387bce41fd4990000000000000000000000000000000000000000000000000000000000000000").to_vec().into(),
                }],
            ),
        ].into_iter().map(|(cumulative_gas_used, logs)| {
            Receipt {
                tx_type: TxType::Legacy,
                success: true,
                cumulative_gas_used,
                bloom: logs_bloom(&logs),
                logs,
            }
        }).collect::<Vec<_>>()), H256(hex!("7ea023138ee7d80db04eeec9cf436dc35806b00cc5fe8e5f611fb7cf1b35b177")))
    }
}
