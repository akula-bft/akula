use crate::models::*;
use async_trait::async_trait;
use ethereum_jsonrpc::NetApiServer;
use jsonrpsee::core::RpcResult;

pub struct NetApiServerImpl;

#[async_trait]
impl NetApiServer for NetApiServerImpl {
    async fn listening(&self) -> RpcResult<bool> {
        Ok(true)
    }
    async fn peer_count(&self) -> RpcResult<U64> {
        Ok(U64::zero())
    }
    async fn version(&self) -> RpcResult<U64> {
        Ok(1.into())
    }
}
