use self::kv_client::*;
use super::*;
use crate::kv::traits::*;
use anyhow::Context;
use async_trait::async_trait;
pub use ethereum_interfaces::remotekv::{Cursor as GrpcCursor, *};
use std::{marker::PhantomData, sync::Arc};
use tokio::sync::{
    mpsc::{channel, Sender},
    oneshot::{channel as oneshot, Sender as OneshotSender},
    Mutex as AsyncMutex,
};
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tonic::{body::BoxBody, client::GrpcService, codegen::Body, Streaming};
use tracing::*;

/// Remote transaction type via gRPC interface.
#[derive(Debug)]
pub struct RemoteTransaction {
    // Invariant: cannot send new message until we process response to it.
    id: u64,
    io: Arc<AsyncMutex<(Sender<GrpcCursor>, Streaming<Pair>)>>,
}

/// Cursor opened by `RemoteTransaction`.
#[derive(Debug)]
pub struct RemoteCursor<'tx, B> {
    transaction: &'tx RemoteTransaction,
    id: u32,

    #[allow(unused)]
    drop_handle: OneshotSender<()>,
    _marker: PhantomData<B>,
}

#[async_trait]
impl<'env> Transaction<'env> for RemoteTransaction {
    type Cursor<'tx, T: Table> = RemoteCursor<'tx, T>;
    type CursorDupSort<'tx, T: DupSort> = RemoteCursor<'tx, T>;

    fn id(&self) -> u64 {
        self.id
    }

    async fn cursor<'tx, T>(&'tx self, table: &T) -> anyhow::Result<Self::Cursor<'tx, T>>
    where
        'env: 'tx,
        T: Table,
    {
        // - send op open
        // - get cursor id
        let mut s = self.io.lock().await;

        let bucket_name = table.db_name().to_string();

        trace!("Sending request to open cursor");

        s.0.send(GrpcCursor {
            op: Op::Open as i32,
            bucket_name,
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
                    io.0.send(GrpcCursor {
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
            _marker: PhantomData,
        })
    }

    async fn cursor_dup_sort<'tx, T>(&'tx self, table: &T) -> anyhow::Result<Self::Cursor<'tx, T>>
    where
        T: DupSort,
    {
        self.cursor(table).await
    }

    async fn get<'tx, T>(&'tx self, table: &T, key: T::Key) -> anyhow::Result<Option<T::Value>>
    where
        'env: 'tx,
        T: Table,
    {
        self.cursor(table)
            .await?
            .op_value(Op::SeekExact, Some(key.encode().as_ref()), None)
            .await
    }
}

impl<'tx, T: Table> RemoteCursor<'tx, T> {
    async fn op(
        &mut self,
        op: Op,
        key: Option<&[u8]>,
        value: Option<&[u8]>,
    ) -> anyhow::Result<Option<Pair>> {
        let mut io = self.transaction.io.lock().await;

        io.0.send(GrpcCursor {
            op: op as i32,
            cursor: self.id,
            k: key.map(|v| v.to_vec().into()).unwrap_or_default(),
            v: value.map(|v| v.to_vec().into()).unwrap_or_default(),

            bucket_name: Default::default(),
        })
        .await?;

        let rsp = io.1.message().await?.context("no response")?;

        Ok((!rsp.k.is_empty() || !rsp.v.is_empty()).then_some(rsp))
    }

    async fn op_none(
        &mut self,
        op: Op,
        key: Option<&[u8]>,
        value: Option<&[u8]>,
    ) -> anyhow::Result<()> {
        self.op(op, key, value).await?;

        Ok(())
    }

    async fn op_value(
        &mut self,
        op: Op,
        key: Option<&[u8]>,
        value: Option<&[u8]>,
    ) -> anyhow::Result<Option<T::Value>> {
        if let Some(rsp) = self.op(op, key, value).await? {
            return Ok(Some(T::Value::decode(&*rsp.v)?));
        }

        Ok(None)
    }

    async fn op_kv(
        &mut self,
        op: Op,
        key: Option<&[u8]>,
        value: Option<&[u8]>,
    ) -> anyhow::Result<Option<(T::Key, T::Value)>>
    where
        T::Key: TableDecode,
    {
        if let Some(rsp) = self.op(op, key, value).await? {
            return Ok(Some((T::Key::decode(&*rsp.k)?, T::Value::decode(&*rsp.v)?)));
        }

        Ok(None)
    }
}

#[async_trait]
impl<'tx, T: Table> traits::Cursor<'tx, T> for RemoteCursor<'tx, T> {
    async fn first(&mut self) -> anyhow::Result<Option<(T::Key, T::Value)>>
    where
        T::Key: TableDecode,
    {
        self.op_kv(Op::First, None, None).await
    }

    async fn seek(&mut self, key: T::SeekKey) -> anyhow::Result<Option<(T::Key, T::Value)>>
    where
        T::Key: TableDecode,
    {
        self.op_kv(Op::Seek, Some(key.encode().as_ref()), None)
            .await
    }

    async fn seek_exact(&mut self, key: T::Key) -> anyhow::Result<Option<(T::Key, T::Value)>>
    where
        T::Key: TableDecode,
    {
        self.op_kv(Op::SeekExact, Some(key.encode().as_ref()), None)
            .await
    }

    async fn next(&mut self) -> anyhow::Result<Option<(T::Key, T::Value)>>
    where
        T::Key: TableDecode,
    {
        self.op_kv(Op::Next, None, None).await
    }

    async fn prev(&mut self) -> anyhow::Result<Option<(T::Key, T::Value)>>
    where
        T::Key: TableDecode,
    {
        self.op_kv(Op::Prev, None, None).await
    }

    async fn last(&mut self) -> anyhow::Result<Option<(T::Key, T::Value)>>
    where
        T::Key: TableDecode,
    {
        self.op_kv(Op::Last, None, None).await
    }

    async fn current(&mut self) -> anyhow::Result<Option<(T::Key, T::Value)>>
    where
        T::Key: TableDecode,
    {
        self.op_kv(Op::Current, None, None).await
    }
}

#[async_trait]
impl<'tx, T: DupSort> traits::CursorDupSort<'tx, T> for RemoteCursor<'tx, T> {
    async fn seek_both_range(
        &mut self,
        key: T::Key,
        value: T::SeekBothKey,
    ) -> anyhow::Result<Option<T::Value>>
    where
        T::Key: Clone,
    {
        Ok(self
            .op_value(
                Op::SeekBoth,
                Some(key.clone().encode().as_ref()),
                Some(value.encode().as_ref()),
            )
            .await?)
    }

    async fn next_dup(&mut self) -> anyhow::Result<Option<(T::Key, T::Value)>>
    where
        T::Key: TableDecode,
    {
        self.op_kv(Op::NextDup, None, None).await
    }
    async fn next_no_dup(&mut self) -> anyhow::Result<Option<(T::Key, T::Value)>>
    where
        T::Key: TableDecode,
    {
        self.op_kv(Op::NextNoDup, None, None).await
    }
}

impl RemoteTransaction {
    pub async fn open<C>(mut client: KvClient<C>) -> anyhow::Result<Self>
    where
        C: GrpcService<BoxBody>,
        <C as GrpcService<BoxBody>>::ResponseBody: Send + Sync + 'static,
        <<C as GrpcService<BoxBody>>::ResponseBody as Body>::Error:
            Into<Box<(dyn std::error::Error + Send + Sync + 'static)>> + Send,
    {
        trace!("Opening transaction");
        let (sender, rx) = channel(1);
        let mut receiver = client.tx(ReceiverStream::new(rx)).await?.into_inner();

        // First message with txid
        let id = receiver.message().await?.context("no response")?.tx_id;

        trace!("Acquired transaction receiver");

        Ok(Self {
            id,
            io: Arc::new(AsyncMutex::new((sender, receiver))),
        })
    }
}
