use super::traits::KV;
use crate::{kv::CustomTable, Cursor, CursorDupSort, Transaction};
use async_trait::async_trait;
use bytes::Bytes;
use ethereum_interfaces::{
    remotekv::{Op, Pair, StateChange},
    types::VersionReply,
};
use futures_core::Stream;
use std::{convert::TryFrom, pin::Pin, sync::Arc};
use tokio::sync::mpsc::channel;
use tokio_stream::StreamExt;
use tonic::Response;

pub struct KvServer<DB: KV + Send + Sync> {
    env: Arc<DB>,
}

#[async_trait]
impl<DB: KV + Send + Sync> ethereum_interfaces::remotekv::kv_server::Kv for KvServer<DB> {
    type TxStream =
        Pin<Box<dyn Stream<Item = Result<Pair, tonic::Status>> + Send + Sync + 'static>>;
    type ReceiveStateChangesStream = tokio_stream::Pending<Result<StateChange, tonic::Status>>;

    async fn version(
        &self,
        _: tonic::Request<()>,
    ) -> Result<Response<VersionReply>, tonic::Status> {
        Ok(Response::new(VersionReply {
            major: 2,
            minor: 0,
            patch: 0,
        }))
    }

    async fn tx(
        &self,
        request: tonic::Request<tonic::Streaming<ethereum_interfaces::remotekv::Cursor>>,
    ) -> Result<Response<Self::TxStream>, tonic::Status> {
        let mut req = request.into_inner();
        let env = self.env.clone();
        let (tx, rx) = channel(1);
        tokio::spawn(async move {
            let dbtx = env.begin(0).await.unwrap();

            let mut cursors: Vec<
                Option<<<DB as KV>::Tx<'_> as Transaction>::CursorDupSort<'_, CustomTable>>,
            > = vec![];

            fn get_cursor<'db: 'tx, 'tx: 'cur, 'cur, DB: KV>(
                cursors: &'cur mut Vec<
                    Option<
                        <<DB as KV>::Tx<'db> as Transaction<'db>>::CursorDupSort<'tx, CustomTable>,
                    >,
                >,
                id: usize,
            ) -> Result<
                &'cur mut <<DB as KV>::Tx<'db> as Transaction<'db>>::CursorDupSort<
                    'tx,
                    CustomTable,
                >,
                tonic::Status,
            > {
                cursors
                    .get_mut(id)
                    .ok_or_else(|| tonic::Status::invalid_argument("invalid cursor"))?
                    .as_mut()
                    .ok_or_else(|| tonic::Status::invalid_argument("cursor closed"))
            }

            while let Some(c) = req.try_next().await.unwrap() {
                let _ = tx
                    .send(
                        async {
                            let cursor_id = c.cursor;
                            let cid = cursor_id as usize;
                            let (k, v) = match Op::from_i32(c.op).ok_or_else(|| {
                                tonic::Status::invalid_argument(format!("invalid op: {}", c.op))
                            })? {
                                Op::First => get_cursor::<DB>(&mut cursors, cid)?
                                    .first()
                                    .await
                                    .map_err(|e| tonic::Status::internal(e.to_string()))?,
                                Op::FirstDup => {
                                    return Err(tonic::Status::unimplemented("not implemented"));
                                }
                                Op::Seek => get_cursor::<DB>(&mut cursors, cid)?
                                    .seek(&*c.k)
                                    .await
                                    .map_err(|e| tonic::Status::internal(e.to_string()))?,
                                Op::SeekBoth => get_cursor::<DB>(&mut cursors, cid)?
                                    .seek_both_range(&*c.k, &*c.v)
                                    .await
                                    .map_err(|e| tonic::Status::internal(e.to_string()))?
                                    .map(|v| (Bytes::new(), v)),
                                Op::Current => get_cursor::<DB>(&mut cursors, cid)?
                                    .current()
                                    .await
                                    .map_err(|e| tonic::Status::internal(e.to_string()))?,
                                Op::Last => get_cursor::<DB>(&mut cursors, cid)?
                                    .last()
                                    .await
                                    .map_err(|e| tonic::Status::internal(e.to_string()))?,
                                Op::LastDup => {
                                    return Err(tonic::Status::unimplemented("not implemented"));
                                }
                                Op::Next => get_cursor::<DB>(&mut cursors, cid)?
                                    .next()
                                    .await
                                    .map_err(|e| tonic::Status::internal(e.to_string()))?,
                                Op::NextDup => get_cursor::<DB>(&mut cursors, cid)?
                                    .next_dup()
                                    .await
                                    .map_err(|e| tonic::Status::internal(e.to_string()))?,
                                Op::NextNoDup => get_cursor::<DB>(&mut cursors, cid)?
                                    .next_no_dup()
                                    .await
                                    .map_err(|e| tonic::Status::internal(e.to_string()))?,
                                Op::Prev => get_cursor::<DB>(&mut cursors, cid)?
                                    .prev()
                                    .await
                                    .map_err(|e| tonic::Status::internal(e.to_string()))?,
                                Op::PrevDup => {
                                    return Err(tonic::Status::unimplemented("not implemented"));
                                }
                                Op::PrevNoDup => {
                                    return Err(tonic::Status::unimplemented("not implemented"));
                                }
                                Op::SeekExact => get_cursor::<DB>(&mut cursors, cid)?
                                    .seek_exact(&*c.k)
                                    .await
                                    .map_err(|e| tonic::Status::internal(e.to_string()))?,
                                Op::SeekBothExact => {
                                    return Err(tonic::Status::unimplemented("not implemented"));
                                }
                                Op::Open => {
                                    let cursor = dbtx
                                        .cursor_dup_sort(&CustomTable::from(c.bucket_name))
                                        .await
                                        .map_err(|e| tonic::Status::internal(e.to_string()))?;
                                    cursors.push(Some(cursor));
                                    let cursor_id = u32::try_from(cursors.len() - 1)
                                        .map_err(|_| tonic::Status::internal("overflow"))?;

                                    return Ok(Pair {
                                        cursor_id,
                                        ..Default::default()
                                    });
                                }
                                Op::Close => {
                                    if let Some(cursor) = cursors.get_mut(c.cursor as usize) {
                                        *cursor = None;
                                    }

                                    return Ok(Pair {
                                        cursor_id: c.cursor,
                                        ..Default::default()
                                    });
                                }
                            }
                            .unwrap_or_else(|| (Bytes::new(), Bytes::new()));

                            Ok(Pair {
                                k: k.as_ref().to_vec().into(),
                                v: v.as_ref().to_vec().into(),
                                cursor_id,
                            })
                        }
                        .await,
                    )
                    .await;
            }
        });

        Ok(Response::new(Box::pin(
            tokio_stream::wrappers::ReceiverStream::new(rx),
        )))
    }

    async fn receive_state_changes(
        &self,
        request: tonic::Request<()>,
    ) -> Result<Response<Self::ReceiveStateChangesStream>, tonic::Status> {
        let _ = request;
        Ok(Response::new(tokio_stream::pending()))
    }
}
