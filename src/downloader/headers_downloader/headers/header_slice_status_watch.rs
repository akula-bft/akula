use super::header_slices::{HeaderSliceStatus, HeaderSlices};
use std::sync::Arc;
use tokio::sync::watch;
use tracing::*;

pub struct HeaderSliceStatusWatch {
    status: HeaderSliceStatus,
    header_slices: Arc<HeaderSlices>,
    pending_watch: watch::Receiver<usize>,
    name: String,
}

impl HeaderSliceStatusWatch {
    pub fn new(status: HeaderSliceStatus, header_slices: Arc<HeaderSlices>, name: &str) -> Self {
        Self {
            status,
            header_slices: header_slices.clone(),
            pending_watch: header_slices.watch_status_changes(status),
            name: String::from(name),
        }
    }

    pub fn pending_count(&self) -> usize {
        self.header_slices.count_slices_in_status(self.status)
    }

    pub async fn wait(&mut self) -> anyhow::Result<()> {
        self.wait_while(0).await
    }

    pub async fn wait_while(&mut self, value: usize) -> anyhow::Result<()> {
        if self.pending_count() == value {
            debug!("{}: waiting pending", self.name);
            while *self.pending_watch.borrow_and_update() == value {
                self.pending_watch.changed().await?;
            }
            debug!("{}: waiting pending done", self.name);
        }
        Ok(())
    }
}
