use super::{super::headers::header::BlockHeader, header_slice_verifier::HeaderSliceVerifier};
use crate::models::{BlockNumber, ChainSpec};
use bytes::Bytes;

#[derive(Debug)]
pub struct HeaderSliceVerifierMock;

impl HeaderSliceVerifierMock {
    pub fn new() -> Self {
        Self {}
    }

    pub fn mark_broken_link(&self, child: &mut BlockHeader) {
        child.header.extra_data = Bytes::from("broken_link");
    }
}

impl HeaderSliceVerifier for HeaderSliceVerifierMock {
    fn verify_link(
        &self,
        child: &BlockHeader,
        _parent: &BlockHeader,
        _chain_spec: &ChainSpec,
    ) -> bool {
        let is_link_broken = child.header.extra_data == "broken_link";
        !is_link_broken
    }

    fn verify_slice(
        &self,
        _headers: &[BlockHeader],
        _start_block_num: BlockNumber,
        _max_timestamp: u64,
        _chain_spec: &ChainSpec,
    ) -> bool {
        true
    }
}
