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

/// Wait on 2 watches at the same time.
/// This is useful to track multiple statuses/slices simultaneously.
pub struct HeaderSliceStatusWatchSelector {
    name: String,
    watch1: HeaderSliceStatusWatch,
    watch2: HeaderSliceStatusWatch,
}

impl HeaderSliceStatusWatchSelector {
    pub fn new(name: &str, watch1: HeaderSliceStatusWatch, watch2: HeaderSliceStatusWatch) -> Self {
        Self {
            name: String::from(name),
            watch1,
            watch2,
        }
    }

    pub fn pending_counts(&self) -> (usize, usize) {
        (self.watch1.pending_count(), self.watch2.pending_count())
    }

    pub async fn wait_while_all(&mut self, values: (usize, usize)) -> anyhow::Result<()> {
        if self.watch1.pending_count() != values.0 {
            return Ok(());
        }
        if self.watch2.pending_count() != values.1 {
            return Ok(());
        }

        debug!("{}: waiting pending", self.name);
        while (*self.watch1.pending_watch.borrow_and_update() == values.0)
            && (*self.watch2.pending_watch.borrow_and_update() == values.1)
        {
            tokio::select! {
                result1 = self.watch1.pending_watch.changed() => {
                    result1?
                }
                result2 = self.watch2.pending_watch.changed() => {
                    result2?
                }
            };
        }
        debug!("{}: waiting pending done", self.name);
        Ok(())
    }
}
