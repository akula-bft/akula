use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

/// Waits for a given timeout, and then ends.
pub struct TimeoutStage {
    timeout: Duration,
    is_over: Arc<AtomicBool>,
}

impl TimeoutStage {
    pub fn new(timeout: Duration) -> Self {
        Self {
            timeout,
            is_over: Arc::new(false.into()),
        }
    }

    pub async fn execute(&mut self) -> anyhow::Result<()> {
        tokio::time::sleep(self.timeout).await;
        self.is_over.store(true, Ordering::SeqCst);
        Ok(())
    }

    pub fn is_over_check(&self) -> impl Fn() -> bool {
        let is_over = self.is_over.clone();
        move || -> bool { is_over.load(Ordering::SeqCst) }
    }

    pub fn can_proceed_check(&self) -> impl Fn() -> bool {
        // this stage can't proceed the slices states
        move || -> bool { false }
    }
}

#[async_trait::async_trait]
impl super::stage::Stage for TimeoutStage {
    async fn execute(&mut self) -> anyhow::Result<()> {
        Self::execute(self).await
    }
    fn can_proceed_check(&self) -> Box<dyn Fn() -> bool + Send> {
        Box::new(Self::can_proceed_check(self))
    }
}
