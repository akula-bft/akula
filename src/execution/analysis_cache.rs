use super::evm::AnalyzedCode;
use ethereum_types::H256;
use lru::LruCache;
use std::num::NonZeroUsize;

#[derive(Debug)]
pub struct AnalysisCache {
    inner: LruCache<H256, AnalyzedCode>,
}

impl Default for AnalysisCache {
    fn default() -> Self {
        Self::new(NonZeroUsize::new(5000).unwrap())
    }
}

impl AnalysisCache {
    pub fn new(cap: NonZeroUsize) -> Self {
        Self {
            inner: LruCache::new(cap),
        }
    }

    pub fn get(&mut self, code_hash: &H256) -> Option<&AnalyzedCode> {
        self.inner.get(code_hash)
    }

    pub fn put(&mut self, code_hash: H256, code: AnalyzedCode) {
        self.inner.put(code_hash, code);
    }
}
