use crate::downloader::{
    messages::Message,
    sentry_client::{MessageFromPeer, PeerFilter, SentryClient, Status},
};
use futures_core::Stream;
use tokio::sync::broadcast;
use tokio_stream::{wrappers, StreamExt};

pub struct SentryClientMock {
    message_sender: Option<broadcast::Sender<MessageFromPeer>>,
    message_receiver: Option<broadcast::Receiver<MessageFromPeer>>,
}

impl SentryClientMock {
    pub fn new() -> Self {
        let (message_sender, message_receiver) = broadcast::channel(10);
        SentryClientMock {
            message_sender: Some(message_sender),
            message_receiver: Some(message_receiver),
        }
    }

    fn stop_receiving_messages(&mut self) {
        self.message_sender = None;
    }
}

#[async_trait::async_trait]
impl SentryClient for SentryClientMock {
    async fn set_status(&mut self, _status: Status) -> anyhow::Result<()> {
        Ok(())
    }

    async fn send_message(
        &mut self,
        _message: Message,
        _peer_filter: PeerFilter,
    ) -> anyhow::Result<()> {
        self.stop_receiving_messages();
        Ok(())
    }

    async fn receive_messages(
        &mut self,
    ) -> anyhow::Result<Box<dyn Stream<Item = anyhow::Result<MessageFromPeer>> + Unpin>> {
        if let Some(receiver) = self.message_receiver.take() {
            let stream = wrappers::BroadcastStream::new(receiver)
                .filter_map(|res| res.ok()) // ignore BroadcastStreamRecvError
                .map(Ok);

            Ok(Box::new(Box::pin(stream)))
        } else {
            anyhow::bail!("SentryClientMock::receive_messages supports only one receiver")
        }
    }
}
