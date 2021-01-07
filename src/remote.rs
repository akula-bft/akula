use self::kv_client::*;
use crate::{dbutils::*, traits};
use anyhow::Context;
use async_stream::{stream, try_stream};
use async_trait::async_trait;
use bytes::Bytes;
use futures::{future::BoxFuture, stream::BoxStream};
use std::sync::Arc;
use tokio::sync::{
    mpsc::{channel, Sender},
    oneshot::{channel as oneshot, Sender as OneshotSender},
    Mutex as AsyncMutex,
};
use tokio_stream::StreamExt;
use tonic::{body::BoxBody, client::GrpcService, codegen::HttpBody, Streaming};
use tracing::*;

tonic::include_proto!("remote");

/// Remote transaction type via gRPC interface.
pub struct RemoteTransaction {
    // Invariant: cannot send new message until we process response to it.
    io: Arc<AsyncMutex<(Sender<Cursor>, Streaming<Pair>)>>,
}

/// Cursor opened by `RemoteTransaction`.
pub struct RemoteCursor<'tx> {
    transaction: &'tx RemoteTransaction,
    id: u32,

    #[allow(unused)]
    drop_handle: OneshotSender<()>,
}

impl crate::Transaction for RemoteTransaction {
    type Cursor<'tx> = RemoteCursor<'tx>;

    fn cursor<'tx>(
        &'tx self,
        bucket_name: &'tx str,
    ) -> BoxFuture<'tx, anyhow::Result<Self::Cursor<'tx>>> {
        Box::pin(async move {
            // - send op open
            // - get cursor id
            let mut s = self.io.lock().await;

            let bucket_name = bucket_name.to_string();

            trace!("Sending request to open cursor");

            s.0.send(Cursor {
                op: Op::Open as i32,
                bucket_name: bucket_name.clone(),
                cursor: Default::default(),
                k: Default::default(),
                v: Default::default(),
            })
            .await?;

            let id = s.1.message().await?.context("no response")?.cursor_id;

            trace!("Opened cursor {}", id);

            drop(s);

            let (drop_handle, drop_rx) = oneshot();

            tokio::spawn({
                let io = self.io.clone();
                async move {
                    let _ = drop_rx.await;
                    let mut io = io.lock().await;

                    trace!("Closing cursor {}", id);
                    let _ =
                        io.0.send(Cursor {
                            op: Op::Close as i32,
                            cursor: id,
                            bucket_name: Default::default(),
                            k: Default::default(),
                            v: Default::default(),
                        })
                        .await;
                    let _ = io.1.next().await;
                }
            });

            Ok(RemoteCursor {
                transaction: self,
                drop_handle,
                id,
            })
        })
    }
}

impl<'tx> RemoteCursor<'tx> {
    async fn op(&mut self, cursor: Cursor) -> anyhow::Result<(Bytes, Bytes)> {
        let mut io = self.transaction.io.lock().await;

        io.0.send(cursor).await?;

        let rsp = io.1.message().await?.context("no response")?;

        Ok((rsp.k.into(), rsp.v.into()))
    }

    fn walk_continue<K: AsRef<[u8]>>(
        k: &[u8],
        fixed_bytes: u64,
        fixed_bits: u64,
        start_key: &K,
        mask: u8,
    ) -> bool {
        !k.is_empty()
            && k.len() as u64 >= fixed_bytes
            && (fixed_bits == 0
                || (k[..fixed_bytes as usize - 1]
                    == start_key.as_ref()[..fixed_bytes as usize - 1])
                    && (k[fixed_bytes as usize - 1] & mask)
                        == (start_key.as_ref()[fixed_bytes as usize - 1] & mask))
    }
}

#[async_trait]
impl<'tx> traits::Cursor for RemoteCursor<'tx> {
    async fn seek_exact(&mut self, key: &[u8]) -> anyhow::Result<(Bytes, Bytes)> {
        self.op(Cursor {
            op: Op::SeekExact as i32,
            cursor: self.id,
            k: key.as_ref().to_vec(),

            bucket_name: Default::default(),
            v: Default::default(),
        })
        .await
    }

    async fn seek(&mut self, key: &[u8]) -> anyhow::Result<(Bytes, Bytes)> {
        self.op(Cursor {
            op: Op::Seek as i32,
            cursor: self.id,
            k: key.as_ref().to_vec(),

            bucket_name: Default::default(),
            v: Default::default(),
        })
        .await
    }

    async fn next(&mut self) -> anyhow::Result<(Bytes, Bytes)> {
        self.op(Cursor {
            op: Op::Next as i32,
            cursor: self.id,

            k: Default::default(),
            bucket_name: Default::default(),
            v: Default::default(),
        })
        .await
    }

    fn walk<'cur>(
        &'cur mut self,
        start_key: &'cur [u8],
        fixed_bits: u64,
    ) -> BoxStream<'cur, anyhow::Result<(Bytes, Bytes)>> {
        Box::pin(try_stream! {
            let (fixed_bytes, mask) = bytes_mask(fixed_bits);

            let (mut k, mut v) = self.seek(start_key).await?;

            while Self::walk_continue(&k, fixed_bytes, fixed_bits, &start_key, mask) {
                yield (k, v);

                let next = self.next().await?;
                k = next.0;
                v = next.1;
            }
        })
    }
}

impl RemoteTransaction {
    pub async fn open<C>(mut client: KvClient<C>) -> anyhow::Result<Self>
    where
        C: GrpcService<BoxBody>,
        <C as GrpcService<BoxBody>>::ResponseBody: 'static,
        <<C as GrpcService<BoxBody>>::ResponseBody as HttpBody>::Error:
            Into<Box<(dyn std::error::Error + Send + Sync + 'static)>> + Send,
    {
        trace!("Opening transaction");
        let (sender, mut rx) = channel(1);
        let mut receiver = client
            .tx(stream! {
                // Just a dummy message, workaround for
                // https://github.com/hyperium/tonic/issues/515
                yield Cursor {
                    op: Op::Open as i32,
                    bucket_name: "DUMMY".into(),
                    cursor: Default::default(),
                    k: Default::default(),
                    v: Default::default(),
                };
                while let Some(v) = rx.recv().await {
                    yield v;
                }
            })
            .await?
            .into_inner();

        // https://github.com/hyperium/tonic/issues/515
        let cursor = receiver.message().await?.context("no response")?.cursor_id;

        sender
            .send(Cursor {
                op: Op::Close as i32,
                cursor,
                bucket_name: Default::default(),
                k: Default::default(),
                v: Default::default(),
            })
            .await?;

        let _ = receiver.try_next().await?;

        trace!("Acquired transaction receiver");

        Ok(Self {
            io: Arc::new(AsyncMutex::new((sender, receiver))),
        })
    }
}
