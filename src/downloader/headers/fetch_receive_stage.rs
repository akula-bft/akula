use crate::{
    downloader::{
        headers::{
            header_slices,
            header_slices::{HeaderSlice, HeaderSliceStatus, HeaderSlices},
        },
        messages::{BlockHeadersMessage, EthMessageId, Message},
        sentry_client_reactor::SentryClientReactor,
    },
    models::{BlockHeader as Header, BlockNumber},
};
use futures_core::Stream;
use parking_lot::RwLock;
use std::{
    ops::DerefMut,
    pin::Pin,
    sync::{atomic::*, Arc},
};
use tokio::sync::Mutex;
use tokio_stream::StreamExt;
use tracing::*;

type BlockHeadersMessageStream = Pin<Box<dyn Stream<Item = BlockHeadersMessage> + Send>>;

/// Receives the slices, and sets Downloaded status.
pub struct FetchReceiveStage {
    header_slices: Arc<HeaderSlices>,
    sentry: Arc<RwLock<SentryClientReactor>>,
    is_over: Arc<AtomicBool>,
    message_stream: Mutex<Option<BlockHeadersMessageStream>>,
}

pub struct CanProceed {
    header_slices: Arc<HeaderSlices>,
    is_over: Arc<AtomicBool>,
}

impl CanProceed {
    pub fn can_proceed(&self) -> bool {
        let cant_receive_more = self.is_over.load(Ordering::SeqCst)
            && self
                .header_slices
                .has_one_of_statuses(&[HeaderSliceStatus::Empty, HeaderSliceStatus::Waiting]);
        !cant_receive_more
    }
}

impl FetchReceiveStage {
    pub fn new(header_slices: Arc<HeaderSlices>, sentry: Arc<RwLock<SentryClientReactor>>) -> Self {
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
            *message_stream = Some(self.receive_headers()?);
        }

        let message_result = message_stream.as_mut().unwrap().next().await;
        match message_result {
            Some(message) => self.on_headers(message.headers),
            None => self.is_over.store(true, Ordering::SeqCst),
        }
        debug!("FetchReceiveStage: done");
        Ok(())
    }

    pub fn can_proceed_checker(&self) -> CanProceed {
        CanProceed {
            is_over: self.is_over.clone(),
            header_slices: self.header_slices.clone(),
        }
    }

    fn on_headers(&self, headers: Vec<Header>) {
        debug!("FetchReceiveStage: received a headers slice");

        if headers.len() < header_slices::HEADER_SLICE_SIZE {
            warn!(
                "FetchReceiveStage got a headers slice of a smaller size: {}",
                headers.len()
            );
            return;
        }
        let start_block_num = headers[0].number;

        let slice_lock_opt = self.header_slices.find_by_start_block_num(start_block_num);
        self.update_slice(slice_lock_opt, headers, start_block_num);
    }

    fn update_slice(
        &self,
        slice_lock_opt: Option<Arc<RwLock<HeaderSlice>>>,
        headers: Vec<Header>,
        start_block_num: BlockNumber,
    ) {
        match slice_lock_opt {
            Some(slice_lock) => {
                let mut slice = slice_lock.write();
                match slice.status {
                    HeaderSliceStatus::Waiting => {
                        slice.headers = Some(headers);
                        self.header_slices
                            .set_slice_status(slice.deref_mut(), HeaderSliceStatus::Downloaded);
                    }
                    unexpected_status => {
                        debug!("FetchReceiveStage ignores a headers slice that we didn't request starting at: {:?}; status = {:?}", start_block_num, unexpected_status);
                    }
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

    fn receive_headers(&self) -> anyhow::Result<BlockHeadersMessageStream> {
        let in_stream = self
            .sentry
            .read()
            .receive_messages(EthMessageId::BlockHeaders)?;

        let out_stream = in_stream.map(|message| match message {
            Message::BlockHeaders(message) => message,
            _ => panic!("unexpected type {:?}", message.eth_id()),
        });

        Ok(Box::pin(out_stream))
    }
}

#[async_trait::async_trait]
impl super::stage::Stage for FetchReceiveStage {
    async fn execute(&mut self) -> anyhow::Result<()> {
        FetchReceiveStage::execute(self).await
    }
}
