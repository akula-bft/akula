use futures_core::Stream;
use std::pin::Pin;

pub type StageStream<'stream> = Pin<Box<dyn Stream<Item = anyhow::Result<()>> + 'stream>>;

pub fn make_stage_stream<'stage>(
    mut stage: Box<dyn super::stage::Stage + 'stage>,
) -> StageStream<'stage> {
    let stream = async_stream::stream! {
        loop {
            yield stage.execute().await;
        }
    };
    Box::pin(stream)
}
