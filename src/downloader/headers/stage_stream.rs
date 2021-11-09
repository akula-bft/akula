use futures_core::Stream;
use std::pin::Pin;

pub type StageStream = Pin<Box<dyn Stream<Item = anyhow::Result<()>>>>;

pub fn make_stage_stream(
    mut stage: Box<dyn crate::downloader::headers::stage::Stage>,
) -> StageStream {
    let stream = async_stream::stream! {
        loop {
            yield stage.execute().await;
        }
    };
    Box::pin(stream)
}
