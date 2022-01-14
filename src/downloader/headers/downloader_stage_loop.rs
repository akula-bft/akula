use super::{
    header_slices::HeaderSlices,
    stage_stream::{make_stage_stream, StageStream},
};
use std::sync::Arc;
use tokio_stream::{StreamExt, StreamMap};
use tracing::*;

// Downloading happens with several stages where
// each of the stages processes blocks in one status,
// and updates them to proceed to the next status.
// All stages run in parallel,
// although most of the time only one of the stages is actively running,
// while the others are waiting for the status updates, IO or timeouts.
pub struct DownloaderStageLoop<'s> {
    header_slices: Arc<HeaderSlices>,
    stream: StreamMap<String, StageStream<'s>>,
}

impl<'s> DownloaderStageLoop<'s> {
    pub fn new(header_slices: &Arc<HeaderSlices>) -> Self {
        Self {
            header_slices: header_slices.clone(),
            stream: StreamMap::<String, StageStream>::new(),
        }
    }

    pub fn insert<Stage: super::stage::Stage + 's>(&mut self, stage: Stage) {
        let name = String::from(std::any::type_name::<Stage>());
        self.stream.insert(name, make_stage_stream(stage));
    }

    pub async fn run(mut self, can_proceed: impl Fn(Arc<HeaderSlices>) -> bool) {
        while let Some((key, result)) = self.stream.next().await {
            if result.is_err() {
                error!("Downloader headers {} failure: {:?}", key, result);
                break;
            }

            if !can_proceed(self.header_slices.clone()) {
                break;
            }

            self.header_slices.notify_status_watchers();
        }
    }
}
