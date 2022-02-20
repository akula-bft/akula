use super::{
    messages::{EthMessageId, Message},
    sentry_client::{MessageFromPeer, MessageFromPeerStream, PeerFilter, SentryClient, Status},
};
use crate::{
    models::{BlockHeader, BlockNumber},
    sentry_connector::{block_id::BlockId, messages::BlockHeadersMessage, sentry_client::PeerId},
};
use std::collections::{HashMap, HashSet};
use tokio::sync::broadcast;
use tokio_stream::{wrappers, StreamExt};

#[derive(Debug)]
pub struct SentryClientMock {
    message_sender: Option<broadcast::Sender<MessageFromPeer>>,
    message_receiver: Option<broadcast::Receiver<MessageFromPeer>>,
    block_headers: HashMap<BlockNumber, Vec<BlockHeader>>,
}

impl SentryClientMock {
    pub fn new() -> Self {
        let (message_sender, message_receiver) = broadcast::channel(10);
        SentryClientMock {
            message_sender: Some(message_sender),
            message_receiver: Some(message_receiver),
            block_headers: HashMap::new(),
        }
    }

    fn stop_receiving_messages(&mut self) {
        self.message_sender = None;
    }

    fn is_empty(&self) -> bool {
        self.block_headers.is_empty()
    }

    pub fn add_block_headers(&mut self, headers: Vec<BlockHeader>) {
        let Some(first_header) = headers.first() else { return };
        let start_block_num = first_header.number;
        self.block_headers.insert(start_block_num, headers);
    }

    pub fn block_headers_mut(
        &mut self,
        start_block_num: BlockNumber,
    ) -> Option<&mut Vec<BlockHeader>> {
        self.block_headers.get_mut(&start_block_num)
    }
}

#[async_trait::async_trait]
impl SentryClient for SentryClientMock {
    async fn set_status(&mut self, _status: Status) -> anyhow::Result<()> {
        Ok(())
    }

    async fn penalize_peer(&mut self, _peer_id: PeerId) -> anyhow::Result<()> {
        Ok(())
    }

    async fn send_message(
        &mut self,
        message: Message,
        _peer_filter: PeerFilter,
    ) -> anyhow::Result<u32> {
        if self.message_sender.is_none() {
            return Ok(0);
        }

        match message {
            Message::GetBlockHeaders(request) => {
                let BlockId::Number(start_block_num) = request.params.start_block else {
                    anyhow::bail!("SentryClientMock::send_message unsupported GetBlockHeaders by hash");
                };
                let block_headers = &mut self.block_headers;
                let Some(headers) = block_headers.remove(&start_block_num) else {
                    return Ok(1);
                };

                let response = BlockHeadersMessage {
                    request_id: request.request_id,
                    headers,
                };

                let response_message = MessageFromPeer {
                    message: Message::BlockHeaders(response),
                    from_peer_id: None,
                };

                self.message_sender
                    .as_ref()
                    .unwrap()
                    .send(response_message)?;
            }
            _ => {
                anyhow::bail!(
                    "SentryClientMock::send_message unsupported message {:?}",
                    message
                )
            }
        }

        if self.is_empty() {
            self.stop_receiving_messages();
        }
        Ok(1)
    }

    async fn receive_messages(
        &mut self,
        filter_ids: &[EthMessageId],
    ) -> anyhow::Result<MessageFromPeerStream> {
        if self.is_empty() {
            return Ok(Box::pin(tokio_stream::empty()));
        }

        let filter_ids_set = filter_ids
            .iter()
            .cloned()
            .collect::<HashSet<EthMessageId>>();

        if let Some(receiver) = self.message_receiver.take() {
            let stream = wrappers::BroadcastStream::new(receiver)
                .filter_map(|res| res.ok()) // ignore BroadcastStreamRecvError
                .filter(move |message_from_peer| {
                    filter_ids_set.is_empty()
                        || filter_ids_set.contains(&message_from_peer.message.eth_id())
                })
                .map(Ok);

            Ok(Box::pin(stream))
        } else {
            anyhow::bail!("SentryClientMock::receive_messages supports only one receiver")
        }
    }
}

impl Default for SentryClientMock {
    fn default() -> Self {
        Self::new()
    }
}
