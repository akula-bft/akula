use crate::downloader::headers::header_slices::{HeaderSliceStatus, HeaderSlices};
use std::{cell::RefCell, sync::Arc};
use tokio::sync::watch;
use tracing::*;

/// Forgets the Saved slices from memory, and creates more Empty slices
/// until we reach the end of pre-verified chain.
pub struct RefillStage {
    header_slices: Arc<HeaderSlices>,
    pending_watch: RefCell<watch::Receiver<usize>>,
}

impl RefillStage {
    pub fn new(header_slices: Arc<HeaderSlices>) -> Self {
        let pending_watch = header_slices.watch_status_changes(HeaderSliceStatus::Saved);

        Self {
            header_slices,
            pending_watch: RefCell::new(pending_watch),
        }
    }

    fn pending_count(&self) -> usize {
        self.header_slices
            .count_slices_in_status(HeaderSliceStatus::Saved)
    }

    pub async fn execute(&self) -> anyhow::Result<()> {
        debug!("RefillStage: start");
        if self.pending_count() == 0 {
            debug!("RefillStage: waiting pending");
            let mut watch = self.pending_watch.borrow_mut();
            while *watch.borrow_and_update() == 0 {
                watch.changed().await?;
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
