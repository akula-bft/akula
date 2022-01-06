use super::{
    header_slice_status_watch::HeaderSliceStatusWatch,
    header_slices::{HeaderSliceStatus, HeaderSlices},
};
use crate::sentry::chain_config::ChainConfig;
use std::sync::Arc;
use tracing::*;

pub struct ForkModeStage {
    header_slices: Arc<HeaderSlices>,
    chain_config: ChainConfig,
    pending_watch: HeaderSliceStatusWatch,
    remaining_count: usize,
}

impl ForkModeStage {
    pub fn new(header_slices: Arc<HeaderSlices>, chain_config: ChainConfig) -> Self {
        Self {
            header_slices: header_slices.clone(),
            chain_config,
            pending_watch: HeaderSliceStatusWatch::new(
                HeaderSliceStatus::VerifiedInternally,
                header_slices,
                "ForkModeStage",
            ),
            remaining_count: 0,
        }
    }

    pub async fn execute(&mut self) -> anyhow::Result<()> {
        debug!("ForkModeStage: start");
        self.pending_watch.wait_while(self.remaining_count).await?;

        // todo!("process");

        self.remaining_count = self.pending_watch.pending_count();
        debug!("ForkModeStage: done");
        Ok(())
    }

    pub fn is_done(&self) -> bool {
        // TODO
        false
    }
}

#[async_trait::async_trait]
impl super::stage::Stage for ForkModeStage {
    async fn execute(&mut self) -> anyhow::Result<()> {
        ForkModeStage::execute(self).await
    }
}
