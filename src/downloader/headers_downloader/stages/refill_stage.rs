use super::headers::{
    header_slice_status_watch::HeaderSliceStatusWatch,
    header_slices::{HeaderSliceStatus, HeaderSlices},
};
use std::sync::Arc;
use tracing::*;

/// Forgets the Saved slices from memory, and creates more Empty slices
/// until we reach the end of pre-verified chain.
pub struct RefillStage {
    header_slices: Arc<HeaderSlices>,
    pending_watch: HeaderSliceStatusWatch,
}

impl RefillStage {
    pub fn new(header_slices: Arc<HeaderSlices>) -> Self {
        Self {
            header_slices: header_slices.clone(),
            pending_watch: HeaderSliceStatusWatch::new(
                HeaderSliceStatus::Saved,
                header_slices,
                "RefillStage",
            ),
        }
    }

    pub async fn execute(&mut self) -> anyhow::Result<()> {
        debug!("RefillStage: start");
        self.pending_watch.wait().await?;

        debug!(
            "RefillStage: refilling {} slices",
            self.pending_watch.pending_count()
        );
        self.refill_pending();
        debug!("RefillStage: done");
        Ok(())
    }

    fn refill_pending(&self) {
        self.header_slices.remove(HeaderSliceStatus::Saved);
        self.header_slices.refill();
    }

    pub fn can_proceed_check(&self) -> impl Fn() -> bool {
        let header_slices = self.header_slices.clone();
        move || -> bool { !header_slices.is_empty_at_final_position() }
    }
}

#[async_trait::async_trait]
impl super::stage::Stage for RefillStage {
    async fn execute(&mut self) -> anyhow::Result<()> {
        RefillStage::execute(self).await
    }
}
