use async_trait::async_trait;

#[async_trait]
pub trait Stage: Send {
    async fn execute(&mut self) -> anyhow::Result<()>;
}
