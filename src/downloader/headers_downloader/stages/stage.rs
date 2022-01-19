use async_trait::async_trait;

#[async_trait]
pub trait Stage: Send {
    async fn execute(&mut self) -> anyhow::Result<()>;
    fn can_proceed_check(&self) -> Box<dyn Fn() -> bool + Send>;
}
