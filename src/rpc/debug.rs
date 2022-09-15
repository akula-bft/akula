use super::helpers;
use crate::kv::{mdbx::*, MdbxWithDirHandle};
use async_trait::async_trait;
use ethereum_interfaces::web3::{
    debug_api_server::DebugApi, AccountStreamRequest, StorageSlot, StorageStreamRequest,
};
use ethereum_jsonrpc::types;
use futures::stream::BoxStream;
use std::sync::Arc;

pub struct DebugApiServerImpl<SE>
where
    SE: EnvironmentKind,
{
    pub db: Arc<MdbxWithDirHandle<SE>>,
}

#[async_trait]
impl<DB> DebugApi for DebugApiServerImpl<DB>
where
    DB: EnvironmentKind,
{
    type AccountStreamStream =
        BoxStream<'static, Result<ethereum_interfaces::web3::Account, tonic::Status>>;
    type StorageStreamStream =
        BoxStream<'static, Result<ethereum_interfaces::web3::StorageSlot, tonic::Status>>;

    async fn account_stream(
        &self,
        request: tonic::Request<AccountStreamRequest>,
    ) -> Result<tonic::Response<Self::AccountStreamStream>, tonic::Status> {
        let AccountStreamRequest { block_id, offset } = request.into_inner();

        let db = self.db.clone();

        let (res_tx, rx) = tokio::sync::mpsc::channel(1);

        tokio::task::spawn_blocking(move || {
            let f = {
                let res_tx = res_tx.clone();
                move || {
                    let tx = db
                        .begin()
                        .map_err(|e| tonic::Status::internal(e.to_string()))?;

                    let block = match helpers::grpc_block_id(
                        block_id
                            .ok_or_else(|| tonic::Status::invalid_argument("expected block id"))?,
                    ) {
                        None
                        | Some(types::BlockId::Number(types::BlockNumber::Latest))
                        | Some(types::BlockId::Number(types::BlockNumber::Pending)) => None,
                        Some(block_id) => {
                            if let Some((block_number, _)) =
                                helpers::resolve_block_id(&tx, block_id)
                                    .map_err(|e| tonic::Status::internal(e.to_string()))?
                            {
                                Some(block_number)
                            } else {
                                return Ok(());
                            }
                        }
                    };

                    let mut it = crate::accessors::state::account::walk(
                        &tx,
                        offset.map(|v| v.into()),
                        block,
                    );

                    while let Some((
                        address,
                        crate::models::Account {
                            nonce,
                            balance,
                            code_hash,
                        },
                    )) = it
                        .next()
                        .transpose()
                        .map_err(|e| tonic::Status::internal(e.to_string()))?
                    {
                        if res_tx
                            .blocking_send(Ok(ethereum_interfaces::web3::Account {
                                address: Some(address.into()),
                                balance: Some(balance.into()),
                                nonce,
                                code: crate::accessors::state::code::read(&tx, code_hash)
                                    .map_err(|e| tonic::Status::internal(e.to_string()))?,
                            }))
                            .is_err()
                        {
                            return Ok(());
                        }
                    }

                    Ok(())
                }
            };
            if let Err::<_, tonic::Status>(e) = (f)() {
                let _ = res_tx.blocking_send(Err(e));
            }
        });

        Ok(tonic::Response::new(Box::pin(
            tokio_stream::wrappers::ReceiverStream::new(rx),
        )))
    }

    async fn storage_stream(
        &self,
        request: tonic::Request<StorageStreamRequest>,
    ) -> Result<tonic::Response<Self::StorageStreamStream>, tonic::Status> {
        let StorageStreamRequest {
            block_id,
            address,
            offset,
        } = request.into_inner();

        let db = self.db.clone();

        let (res_tx, rx) = tokio::sync::mpsc::channel(1);

        tokio::task::spawn_blocking(move || {
            let f = {
                let res_tx = res_tx.clone();
                move || {
                    let tx = db
                        .begin()
                        .map_err(|e| tonic::Status::internal(e.to_string()))?;

                    let block = match helpers::grpc_block_id(
                        block_id
                            .ok_or_else(|| tonic::Status::invalid_argument("expected block id"))?,
                    ) {
                        None
                        | Some(types::BlockId::Number(types::BlockNumber::Latest))
                        | Some(types::BlockId::Number(types::BlockNumber::Pending)) => None,
                        Some(block_id) => {
                            if let Some((block_number, _)) =
                                helpers::resolve_block_id(&tx, block_id)
                                    .map_err(|e| tonic::Status::internal(e.to_string()))?
                            {
                                Some(block_number)
                            } else {
                                return Ok(());
                            }
                        }
                    };

                    let mut it = crate::accessors::state::storage::walk(
                        &tx,
                        address
                            .ok_or_else(|| tonic::Status::invalid_argument("expected address"))?
                            .into(),
                        offset.map(|v| v.into()),
                        block,
                    );

                    while let Some((key, value)) = it
                        .next()
                        .transpose()
                        .map_err(|e| tonic::Status::internal(e.to_string()))?
                    {
                        if res_tx
                            .blocking_send(Ok(StorageSlot {
                                key: Some(key.into()),
                                value: Some(value.into()),
                            }))
                            .is_err()
                        {
                            return Ok(());
                        }
                    }

                    Ok(())
                }
            };
            if let Err::<_, tonic::Status>(e) = (f)() {
                let _ = res_tx.blocking_send(Err(e));
            }
        });

        Ok(tonic::Response::new(Box::pin(
            tokio_stream::wrappers::ReceiverStream::new(rx),
        )))
    }
}
