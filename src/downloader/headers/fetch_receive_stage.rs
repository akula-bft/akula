use crate::{
    downloader::headers::{
        header_slices,
        header_slices::{HeaderSlice, HeaderSliceStatus, HeaderSlices},
    },
    models::BlockHeader as Header,
    sentry::{
        messages::{BlockHeadersMessage, EthMessageId, Message},
        sentry_client::PeerId,
        sentry_client_reactor::*,
    },
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
        debug!("FetchReceiveStage: start");
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
        debug!("FetchReceiveStage: done");
        Ok(())
    }

    fn on_headers_message(&self, message_from_peer: BlockHeadersMessageFromPeer) {
        debug!("FetchReceiveStage: received a headers slice");

        let headers = message_from_peer.message.headers;
        if headers.len() < header_slices::HEADER_SLICE_SIZE {
            warn!(
                "FetchReceiveStage got a headers slice of a smaller size: {}",
                headers.len()
            );
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
        headers: Vec<Header>,
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
        let check = FetchReceiveStageCanProceedCheck {
            header_slices: self.header_slices.clone(),
            is_over: self.is_over.clone(),
        };
        move || -> bool { check.can_proceed() }
    }
}

struct FetchReceiveStageCanProceedCheck {
    header_slices: Arc<HeaderSlices>,
    is_over: Arc<AtomicBool>,
}

impl FetchReceiveStageCanProceedCheck {
    pub fn can_proceed(&self) -> bool {
        let cant_receive_more = self.is_over.load(Ordering::SeqCst)
            && self
                .header_slices
                .has_one_of_statuses(&[HeaderSliceStatus::Empty, HeaderSliceStatus::Waiting]);
        !cant_receive_more
    }
}

#[async_trait::async_trait]
impl super::stage::Stage for FetchReceiveStage {
    async fn execute(&mut self) -> anyhow::Result<()> {
        FetchReceiveStage::execute(self).await
    }
}
