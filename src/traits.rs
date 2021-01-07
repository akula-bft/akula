use async_trait::async_trait;
use bytes::Bytes;
use futures::{future::BoxFuture, stream::BoxStream};

pub trait Transaction: Send {
    type Cursor<'tx>: Cursor;

    /// Open the cursor to interact with the bucket.
    fn cursor<'tx>(
        &'tx self,
        bucket_name: &'tx str,
    ) -> BoxFuture<'tx, anyhow::Result<Self::Cursor<'tx>>>;
}

#[async_trait]
pub trait Cursor: Send {
    async fn seek_exact(&mut self, key: &[u8]) -> anyhow::Result<(Bytes, Bytes)>;
    async fn seek(&mut self, key: &[u8]) -> anyhow::Result<(Bytes, Bytes)>;
    async fn next(&mut self) -> anyhow::Result<(Bytes, Bytes)>;
    fn walk<'cur>(
        &'cur mut self,
        start_key: &'cur [u8],
        fixed_bits: u64,
    ) -> BoxStream<'cur, anyhow::Result<(Bytes, Bytes)>>;
}
