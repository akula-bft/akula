use crate::version_string;
use async_trait::async_trait;
use ethereum_jsonrpc::Web3ApiServer;
use jsonrpsee::core::RpcResult;

pub struct Web3ApiServerImpl;

#[async_trait]
impl Web3ApiServer for Web3ApiServerImpl {
    async fn client_version(&self) -> RpcResult<String> {
        Ok(version_string())
    }
}
