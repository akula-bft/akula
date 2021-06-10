use http::Uri;
use std::str::FromStr;

#[derive(Debug)]
pub struct SentryAddress {
    pub addr: Uri,
}

impl FromStr for SentryAddress {
    type Err = anyhow::Error;

    fn from_str(addr_str: &str) -> anyhow::Result<Self> {
        let addr_uri: Uri = addr_str.parse()?;
        Ok(SentryAddress { addr: addr_uri })
    }
}
