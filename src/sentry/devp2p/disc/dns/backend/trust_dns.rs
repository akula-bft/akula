use super::Backend;
use async_trait::async_trait;
use tracing::*;
use trust_dns_resolver::{
    error::{ResolveError, ResolveErrorKind},
    proto::DnsHandle,
    AsyncResolver, ConnectionProvider,
};

#[async_trait]
impl<C, P> Backend for AsyncResolver<C, P>
where
    C: DnsHandle<Error = ResolveError>,
    P: ConnectionProvider<Conn = C>,
{
    async fn get_record(&self, fqdn: String) -> anyhow::Result<Option<String>> {
        trace!("Resolving FQDN {}", fqdn);
        match self.txt_lookup(format!("{}.", fqdn)).await {
            Err(e) => {
                if !matches!(e.kind(), ResolveErrorKind::NoRecordsFound { .. }) {
                    return Err(e.into());
                }
            }
            Ok(v) => {
                if let Some(txt) = v.into_iter().next() {
                    if let Some(txt_entry) = txt.iter().next() {
                        return Ok(Some(String::from_utf8(txt_entry.to_vec())?));
                    }
                }
            }
        }

        Ok(None)
    }
}
