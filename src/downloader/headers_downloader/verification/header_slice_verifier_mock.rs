use super::{
    super::headers::header::BlockHeader, header_slice_verifier::HeaderSliceVerifier,
    preverified_hashes_config::PreverifiedHashesConfig,
};
use crate::models::{BlockNumber, ChainSpec};
use std::fmt::{Debug, Formatter};

pub struct HeaderSliceVerifierMock {
    header_id: fn(header: &BlockHeader) -> u64,
}

impl Debug for HeaderSliceVerifierMock {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HeaderSliceVerifierMock").finish()
    }
}

impl HeaderSliceVerifierMock {
    pub fn new(header_id: fn(header: &BlockHeader) -> u64) -> Self {
        Self { header_id }
    }
}

impl HeaderSliceVerifier for HeaderSliceVerifierMock {
    fn verify_link(
        &self,
        child: &BlockHeader,
        parent: &BlockHeader,
        _chain_spec: &ChainSpec,
    ) -> bool {
        let child_id = (self.header_id)(child);
        let parent_id = (self.header_id)(parent);
        child_id == parent_id + 1
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

    fn preverified_hashes_config(
        &self,
        _chain_name: &str,
    ) -> anyhow::Result<PreverifiedHashesConfig> {
        Ok(PreverifiedHashesConfig::empty())
    }
}
