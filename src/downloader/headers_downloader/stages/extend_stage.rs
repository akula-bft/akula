use super::headers::{
    header_slice_status_watch::HeaderSliceStatusWatch,
    header_slices::{HeaderSliceStatus, HeaderSlices},
};
use std::sync::Arc;
use tracing::*;

/// Extends the slices when it's full with Saved slices.
pub struct ExtendStage {
    header_slices: Arc<HeaderSlices>,
    pending_watch: HeaderSliceStatusWatch,
    remaining_count: usize,
}

impl ExtendStage {
    pub fn new(header_slices: Arc<HeaderSlices>) -> Self {
        Self {
            header_slices: header_slices.clone(),
            pending_watch: HeaderSliceStatusWatch::new(
                HeaderSliceStatus::Saved,
                header_slices,
                "ExtendStage",
            ),
            remaining_count: 0,
        }
    }

    pub async fn execute(&mut self) -> anyhow::Result<()> {
        // If the number of Saved slices (pending_count) is not sufficient for extension,
        // we need to wait until it increases.
        // If it is sufficient (equal to header_slices len),
        // then after extension it becomes insufficient,
        // and we need to wait until it increases anyway.
        // remaining_count can't suddenly become sufficient as long as
        // this is the only stage that can change the header_slices len.
        self.pending_watch.wait_while(self.remaining_count).await?;
        self.remaining_count = self.pending_watch.pending_count();

        if self.header_slices.all_in_status(HeaderSliceStatus::Saved) {
            debug!("ExtendStage: extending full header slices");
            self.header_slices.append_slice()?;
        }

        Ok(())
    }

    pub fn can_proceed_check(&self) -> impl Fn() -> bool {
        let header_slices = self.header_slices.clone();
        move || -> bool { header_slices.all_in_status(HeaderSliceStatus::Saved) }
    }
}

#[async_trait::async_trait]
impl super::stage::Stage for ExtendStage {
    async fn execute(&mut self) -> anyhow::Result<()> {
        Self::execute(self).await
    }
    fn can_proceed_check(&self) -> Box<dyn Fn() -> bool + Send> {
        Box::new(Self::can_proceed_check(self))
    }
}
