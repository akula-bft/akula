use super::{headers::header_slices::HeaderSlices, stages::stage::Stage as DownloaderStage};
use futures_core::Stream;
use std::{any::type_name, collections::HashMap, pin::Pin, sync::Arc};
use tokio_stream::{StreamExt, StreamMap};
use tracing::*;

type StageStream<'a> = Pin<Box<dyn Stream<Item = anyhow::Result<()>> + 'a + Send>>;

fn make_stage_stream<'a, Stage: DownloaderStage + 'a>(
    mut stage: Stage,
    stage_name: String,
) -> StageStream<'a> {
    let stream = async_stream::stream! {
        loop {
            debug!("{}: start", stage_name);
            let result = stage.execute().await;
            debug!("{}: done", stage_name);
            yield result;
        }
    };
    Box::pin(stream)
}

fn short_stage_name<Stage: DownloaderStage>() -> &'static str {
    let mut name = type_name::<Stage>();
    // trim generic part
    name = &name[0..name.find('<').unwrap_or(name.len())];
    // trim module names part
    name = &name[name.rfind(':').map(|pos| pos + 1).unwrap_or(0)..];
    name
}

// Downloading happens with several stages where
// each of the stages processes blocks in one status,
// and updates them to proceed to the next status.
// All stages run in parallel,
// although most of the time only one of the stages is actively running,
// while the others are waiting for the status updates, IO or timeouts.
pub struct DownloaderStageLoop<'s> {
    header_slices: Arc<HeaderSlices>,
    fork_header_slices: Option<Arc<HeaderSlices>>,
    stream: StreamMap<String, StageStream<'s>>,
    stages_can_proceed: HashMap<String, Box<dyn Fn() -> bool + Send>>,
}

impl<'s> DownloaderStageLoop<'s> {
    pub fn new(
        header_slices: &Arc<HeaderSlices>,
        fork_header_slices: Option<&Arc<HeaderSlices>>,
    ) -> Self {
        Self {
            header_slices: header_slices.clone(),
            fork_header_slices: fork_header_slices.cloned(),
            stream: StreamMap::<String, StageStream>::new(),
            stages_can_proceed: HashMap::new(),
        }
    }

    pub fn insert<Stage: DownloaderStage + 's>(&mut self, stage: Stage) {
        let name = String::from(short_stage_name::<Stage>());
        self.insert_with_name(stage, name);
    }

    pub fn insert_with_group_name<Stage: DownloaderStage + 's>(
        &mut self,
        stage: Stage,
        group_name: &str,
    ) {
        let name = format!("{}.{}", group_name, short_stage_name::<Stage>());
        self.insert_with_name(stage, name);
    }

    fn insert_with_name<Stage: DownloaderStage + 's>(&mut self, stage: Stage, name: String) {
        self.stages_can_proceed
            .insert(name.clone(), stage.can_proceed_check());
        self.stream
            .insert(name.clone(), make_stage_stream(stage, name));
    }

    fn some_stage_can_proceed(&self) -> bool {
        self.stages_can_proceed
            .iter()
            .any(|(_, can_proceed)| can_proceed())
    }

    fn find_stage_name_can_proceed(&self) -> Option<&str> {
        self.stages_can_proceed
            .iter()
            .find_map(|(name, can_proceed)| {
                if can_proceed() {
                    Some(name.as_str())
                } else {
                    None
                }
            })
    }

    pub async fn run(mut self, is_over_check: impl Fn() -> bool) {
        while let Some((key, result)) = self.stream.next().await {
            if result.is_err() {
                error!("Downloader headers {} failure: {:?}", key, result);
                break;
            }

            let is_over = is_over_check();
            let can_proceed = !is_over && self.some_stage_can_proceed();
            if !can_proceed {
                break;
            }

            trace!(
                "DownloaderStageLoop: {:?} can proceed",
                self.find_stage_name_can_proceed()
            );
            trace!(
                "DownloaderStageLoop: statuses = {:?}",
                self.header_slices.clone_statuses()
            );
            if let Some(fork_header_slices) = &self.fork_header_slices {
                trace!(
                    "DownloaderStageLoop: fork statuses = {:?}",
                    fork_header_slices.clone_statuses()
                );
            }

            self.header_slices.notify_status_watchers();

            if let Some(fork_header_slices) = &self.fork_header_slices {
                fork_header_slices.notify_status_watchers();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_short_stage_name() {
        assert_eq!(
            short_stage_name::<super::super::stages::RefillStage>(),
            "RefillStage"
        );
    }
}
