use futures_core::Stream;
use std::pin::Pin;

pub type StageStream<'a> = Pin<Box<dyn Stream<Item = anyhow::Result<()>> + 'a + Send>>;

pub fn make_stage_stream<'a>(mut stage: impl super::stage::Stage + 'a) -> StageStream<'a> {
    let stream = async_stream::stream! {
        loop {
            yield stage.execute().await;
        }
    };
    Box::pin(stream)
}
