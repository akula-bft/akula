use super::{message::*, util::*, NodeId, NodeRecord};
use array_init::array_init;
use arrayvec::ArrayVec;
use primitive_types::H256;
use std::{
    collections::{BTreeMap, VecDeque},
    convert::TryFrom,
};
use tracing::*;

pub const BUCKET_SIZE: usize = 16;
pub const REPLACEMENTS_SIZE: usize = 16;

const ADDRESS_BYTES_SIZE: usize = 32;
pub const ADDRESS_BITS: usize = 8 * ADDRESS_BYTES_SIZE;

pub fn distance(n1: NodeId, n2: NodeId) -> H256 {
    keccak256(n1) ^ keccak256(n2)
}

pub type NodeBucket = ArrayVec<NodeRecord, BUCKET_SIZE>;

#[derive(Debug, Default)]
pub struct KBucket {
    bucket: VecDeque<NodeRecord>,
    replacements: VecDeque<NodeRecord>,
}

impl KBucket {
    pub fn find_peer_pos(&self, peer: NodeId) -> Option<usize> {
        (0..self.bucket.len()).find(|&i| self.bucket[i].id == peer)
    }

    pub fn push_replacement(&mut self, peer: NodeRecord) {
        if self.replacements.len() < REPLACEMENTS_SIZE {
            self.replacements.push_back(peer)
        }
    }
}

#[derive(Debug)]
pub struct Table {
    id_hash: H256,
    kbuckets: [KBucket; ADDRESS_BITS],
}

impl Table {
    pub fn new(id: NodeId) -> Self {
        Self {
            id_hash: keccak256(id),
            kbuckets: array_init(|_| Default::default()),
        }
    }

    fn logdistance(&self, peer: NodeId) -> Option<usize> {
        let remote_hash = keccak256(peer);
        for i in (0..ADDRESS_BYTES_SIZE).rev() {
            let byte_index = ADDRESS_BYTES_SIZE - i - 1;
            let d = self.id_hash[byte_index] ^ remote_hash[byte_index];
            if d != 0 {
                let high_bit_index = 7 - d.leading_zeros() as usize;
                return Some(i * 8 + high_bit_index);
            }
        }
        None // n1 and n2 are equal, so logdistance is -inf
    }

    fn bucket(&self, peer: NodeId) -> Option<(usize, &KBucket)> {
        self.logdistance(peer)
            .map(|bucket_idx| (bucket_idx, &self.kbuckets[bucket_idx]))
    }

    fn bucket_mut(&mut self, peer: NodeId) -> Option<(usize, &mut KBucket)> {
        if let Some(bucket_idx) = self.logdistance(peer) {
            return Some((bucket_idx, &mut self.kbuckets[bucket_idx]));
        }

        None
    }

    pub fn get(&self, peer: NodeId) -> Option<Endpoint> {
        self.bucket(peer).and_then(|(_, bucket)| {
            bucket
                .bucket
                .iter()
                .find(|entry| entry.id == peer)
                .copied()
                .map(From::from)
        })
    }

    pub fn filled_buckets(&self) -> Vec<u8> {
        self.kbuckets
            .iter()
            .enumerate()
            .filter_map(|(i, kbucket)| {
                if kbucket.bucket.len() >= BUCKET_SIZE {
                    Some(u8::try_from(i).expect("there are only 255 kbuckets"))
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn oldest(&self, bucket_no: u8) -> Option<NodeRecord> {
        self.kbuckets[bucket_no as usize]
            .bucket
            .iter()
            .next_back()
            .copied()
    }

    /// Add verified node if there is space.
    #[instrument(skip_all, fields(node = &*node.id.to_string()))]
    pub fn add_verified(&mut self, node: NodeRecord) {
        trace!("Adding peer");
        if node.address.is_ipv6() {
            return;
        }

        if let Some((bucket_idx, bucket)) = self.bucket_mut(node.id) {
            trace!("Adding to bucket: {bucket_idx}");
            if let Some(pos) = bucket.find_peer_pos(node.id) {
                bucket.bucket.remove(pos);
            }

            // Push to front of bucket if we have less than BUCKET_SIZE peers, or we are shuffling existing peer...
            if bucket.bucket.len() < BUCKET_SIZE {
                bucket.bucket.push_front(node);
            } else {
                // ...add to replacements otherwise
                bucket.push_replacement(node);
            }
        }
    }

    /// Add seen node if there is space.
    #[instrument(skip_all, fields(node = &*node.id.to_string()))]
    pub fn add_seen(&mut self, node: NodeRecord) {
        trace!("Adding peer");
        if node.address.is_ipv6() {
            return;
        }

        if let Some((bucket_idx, bucket)) = self.bucket_mut(node.id) {
            trace!("Adding peer to bucket {bucket_idx}");
            if bucket.find_peer_pos(node.id).is_some() {
                // Peer exists already, do nothing
                return;
            }

            // Push to back of bucket if we have less than BUCKET_SIZE peers...
            if bucket.bucket.len() < BUCKET_SIZE {
                bucket.bucket.push_back(node);
            } else {
                // ...add to replacements otherwise
                bucket.push_replacement(node);
            }
        }
    }

    /// Remove node from the bucket
    #[instrument(skip_all, fields(node = &*node.to_string()))]
    pub fn remove(&mut self, node: NodeId) {
        if let Some((bucket_idx, bucket)) = self.bucket_mut(node) {
            if bucket.replacements.is_empty() {
                trace!("Not removing from bucket {bucket_idx}: no replacements");
                return;
            }

            for i in 0..bucket.bucket.len() {
                if bucket.bucket[i].id == node {
                    let replacement = bucket
                        .replacements
                        .pop_front()
                        .expect("already returned if no replacement");
                    trace!("Replacing in bucket {bucket_idx} with {:?}", replacement);
                    bucket.bucket.remove(i);
                    bucket.bucket.push_back(replacement);

                    return;
                }
            }
        }
    }

    pub fn neighbours(&self, peer: NodeId) -> Option<NodeBucket> {
        self.bucket(peer).map(|(_, bucket)| {
            bucket
                .bucket
                .iter()
                .filter_map(|neighbour| {
                    if peer == neighbour.id {
                        None
                    } else {
                        Some(*neighbour)
                    }
                })
                .collect()
        })
    }

    pub fn nearest_node_entries(&self, target: NodeId) -> BTreeMap<H256, NodeRecord> {
        self.kbuckets
            .iter()
            .flat_map(|bucket| &bucket.bucket)
            .map(|n| (distance(n.id, target), *n))
            .collect()
    }

    pub fn len(&self) -> usize {
        self.kbuckets
            .iter()
            .fold(0, |total, bucket| total + bucket.bucket.len())
    }
}
