use super::Backend;
use async_trait::async_trait;
use std::collections::HashMap;
use tracing::*;

#[async_trait]
impl Backend for HashMap<String, String> {
    async fn get_record(&self, fqdn: String) -> anyhow::Result<Option<String>> {
        debug!("resolving {}", fqdn);
        if let Some(v) = self.get(&fqdn) {
            debug!("resolved {} to {}", fqdn, v);
            return Ok(Some(v.clone()));
        }

        Ok(None)
    }
}
