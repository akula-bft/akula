use super::{headers::header_slices::HeaderSlices, stages::stage::Stage as DownloaderStage};
use futures_core::Stream;
use std::{pin::Pin, sync::Arc};
use tokio_stream::{StreamExt, StreamMap};
use tracing::*;

type StageStream<'a> = Pin<Box<dyn Stream<Item = anyhow::Result<()>> + 'a + Send>>;

fn make_stage_stream<'a>(mut stage: impl DownloaderStage + 'a) -> StageStream<'a> {
    let stream = async_stream::stream! {
        loop {
            yield stage.execute().await;
        }
    };
    Box::pin(stream)
}

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

    pub fn insert<Stage: DownloaderStage + 's>(&mut self, stage: Stage) {
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
