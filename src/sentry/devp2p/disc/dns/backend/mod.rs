use async_trait::async_trait;
use auto_impl::auto_impl;

pub mod memory;
pub mod trust_dns;

#[async_trait]
#[auto_impl(&, Box, Arc)]
pub trait Backend: Send + Sync + 'static {
    async fn get_record(&self, fqdn: String) -> anyhow::Result<Option<String>>;
}
