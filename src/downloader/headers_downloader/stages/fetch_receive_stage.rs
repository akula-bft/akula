use super::headers::{
    header::BlockHeader,
    header_slices::{HeaderSlice, HeaderSliceStatus, HeaderSlices},
};
use crate::sentry_connector::{
    messages::{BlockHeadersMessage, EthMessageId, Message},
    sentry_client::PeerId,
    sentry_client_reactor::*,
};
use futures_core::Stream;
use std::{
    ops::DerefMut,
    pin::Pin,
    sync::{atomic::*, Arc},
};
use tokio::sync::Mutex;
use tokio_stream::StreamExt;
use tracing::*;

struct BlockHeadersMessageFromPeer {
    message: BlockHeadersMessage,
    from_peer_id: Option<PeerId>,
}

type BlockHeadersMessageStream = Pin<Box<dyn Stream<Item = BlockHeadersMessageFromPeer> + Send>>;

/// Receives the slices, and sets Downloaded status.
pub struct FetchReceiveStage {
    header_slices: Arc<HeaderSlices>,
    sentry: SentryClientReactorShared,
    is_over: Arc<AtomicBool>,
    message_stream: Mutex<Option<BlockHeadersMessageStream>>,
}

impl FetchReceiveStage {
    pub fn new(header_slices: Arc<HeaderSlices>, sentry: SentryClientReactorShared) -> Self {
        Self {
            header_slices,
            sentry,
            is_over: Arc::new(false.into()),
            message_stream: Mutex::new(None),
        }
    }

    pub async fn execute(&self) -> anyhow::Result<()> {
        // when it is over - hang to avoid a live idle loop
        // ideally stop calling this execute() in the make_stage_stream loop
        if self.is_over.load(Ordering::SeqCst) {
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        }

        let mut message_stream = self.message_stream.try_lock()?;
        if message_stream.is_none() {
            let sentry = self.sentry.read().await;
            *message_stream = Some(self.receive_headers(&sentry)?);
        }

        let message_result = message_stream.as_mut().unwrap().next().await;
        match message_result {
            Some(message) => self.on_headers_message(message),
            None => self.is_over.store(true, Ordering::SeqCst),
        }
        Ok(())
    }

    fn on_headers_message(&self, message_from_peer: BlockHeadersMessageFromPeer) {
        debug!("FetchReceiveStage: received a headers slice");

        let headers = message_from_peer.message.headers;
        if headers.is_empty() {
            warn!("FetchReceiveStage got an empty slice");
            return;
        }

        let start_block_num = headers[0].number;
        let slice_lock_opt = self.header_slices.find_by_start_block_num(start_block_num);

        match slice_lock_opt {
            Some(slice_lock) => {
                let mut slice = slice_lock.write();
                let slice_status = slice.status;
                if slice_status == HeaderSliceStatus::Waiting {
                    let from_peer_id = message_from_peer.from_peer_id;
                    let headers: Vec<BlockHeader> =
                        headers.into_iter().map(BlockHeader::from).collect();
                    self.update_slice(slice.deref_mut(), headers, from_peer_id);
                } else {
                    debug!("FetchReceiveStage ignores a headers slice that we didn't request starting at: {:?}; status = {:?}", start_block_num, slice_status);
                }
            }
            None => {
                debug!(
                    "FetchReceiveStage ignores a headers slice that we didn't request starting at: {:?}",
                    start_block_num
                );
            }
        }
    }

    fn update_slice(
        &self,
        slice: &mut HeaderSlice,
        headers: Vec<BlockHeader>,
        from_peer_id: Option<PeerId>,
    ) {
        slice.headers = Some(headers);
        slice.from_peer_id = from_peer_id;
        self.header_slices
            .set_slice_status(slice, HeaderSliceStatus::Downloaded);
    }

    fn receive_headers(
        &self,
        sentry: &SentryClientReactor,
    ) -> anyhow::Result<BlockHeadersMessageStream> {
        let in_stream = sentry.receive_messages(EthMessageId::BlockHeaders)?;

        let out_stream = in_stream.map(|message_from_peer| match message_from_peer.message {
            Message::BlockHeaders(message) => BlockHeadersMessageFromPeer {
                message,
                from_peer_id: message_from_peer.from_peer_id,
            },
            _ => panic!("unexpected type {:?}", message_from_peer.message.eth_id()),
        });

        Ok(Box::pin(out_stream))
    }

    pub fn can_proceed_check(&self) -> impl Fn() -> bool {
        let header_slices = self.header_slices.clone();
        let is_over = self.is_over.clone();
        move || -> bool {
            !is_over.load(Ordering::SeqCst)
                && header_slices.contains_status(HeaderSliceStatus::Waiting)
        }
    }
}

#[async_trait::async_trait]
impl super::stage::Stage for FetchReceiveStage {
    async fn execute(&mut self) -> anyhow::Result<()> {
        Self::execute(self).await
    }
    fn can_proceed_check(&self) -> Box<dyn Fn() -> bool + Send> {
        Box::new(Self::can_proceed_check(self))
    }
}
