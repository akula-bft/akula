use super::evm::AnalyzedCode;
use ethereum_types::H256;
use lru::LruCache;

#[derive(Debug)]
pub struct AnalysisCache {
    inner: LruCache<H256, AnalyzedCode>,
}

impl Default for AnalysisCache {
    fn default() -> Self {
        Self::new(5000)
    }
}

impl AnalysisCache {
    pub fn new(cap: usize) -> Self {
        Self {
            inner: LruCache::new(cap),
        }
    }

    pub fn get(&mut self, code_hash: H256) -> Option<&AnalyzedCode> {
        self.inner.get(&code_hash)
    }

    pub fn put(&mut self, code_hash: H256, code: AnalyzedCode) {
        self.inner.put(code_hash, code);
    }
}
