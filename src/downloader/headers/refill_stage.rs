use crate::downloader::headers::header_slices::{HeaderSliceStatus, HeaderSlices};
use std::sync::Arc;
use tokio::sync::watch;
use tracing::*;

/// Forgets the Saved slices from memory, and creates more Empty slices
/// until we reach the end of pre-verified chain.
pub struct RefillStage {
    header_slices: Arc<HeaderSlices>,
    pending_watch: watch::Receiver<usize>,
}

impl RefillStage {
    pub fn new(header_slices: Arc<HeaderSlices>) -> Self {
        Self {
            pending_watch: header_slices.watch_status_changes(HeaderSliceStatus::Saved),
            header_slices,
        }
    }

    fn pending_count(&self) -> usize {
        self.header_slices
            .count_slices_in_status(HeaderSliceStatus::Saved)
    }

    pub async fn execute(&mut self) -> anyhow::Result<()> {
        debug!("RefillStage: start");
        if self.pending_count() == 0 {
            debug!("RefillStage: waiting pending");
            while *self.pending_watch.borrow_and_update() == 0 {
                self.pending_watch.changed().await?;
            }
            debug!("RefillStage: waiting pending done");
        }

        info!("RefillStage: refilling {} slices", self.pending_count());
        self.refill_pending();
        debug!("RefillStage: done");
        Ok(())
    }

    fn refill_pending(&self) {
        self.header_slices.remove(HeaderSliceStatus::Saved);
        self.header_slices.refill();
    }
}
