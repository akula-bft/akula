use async_trait::async_trait;

#[async_trait]
pub trait Stage {
    async fn execute(&mut self) -> anyhow::Result<()>;
}
