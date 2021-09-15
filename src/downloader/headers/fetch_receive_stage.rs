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
    cell::{Cell, RefCell},
    ops::DerefMut,
    pin::Pin,
    sync::Arc,
};
use tokio_stream::StreamExt;
use tracing::*;

type BlockHeadersMessageStream = Pin<Box<dyn Stream<Item = BlockHeadersMessage> + Send>>;

/// Receives the slices, and sets Downloaded status.
pub struct FetchReceiveStage {
    header_slices: Arc<HeaderSlices>,
    sentry: Arc<RwLock<SentryClientReactor>>,
    is_over: Cell<bool>,
    message_stream: RefCell<Option<RefCell<BlockHeadersMessageStream>>>,
}

impl FetchReceiveStage {
    pub fn new(header_slices: Arc<HeaderSlices>, sentry: Arc<RwLock<SentryClientReactor>>) -> Self {
        Self {
            header_slices,
            sentry,
            is_over: Cell::new(false),
            message_stream: RefCell::new(None),
        }
    }

    pub async fn execute(&self) -> anyhow::Result<()> {
        debug!("FetchReceiveStage: start");
        if self.message_stream.borrow().is_none() {
            let message_stream = self.receive_headers()?;
            *self.message_stream.borrow_mut() = Some(RefCell::new(message_stream));
        }

        let message_stream_opt = self.message_stream.borrow();
        let mut message_stream = message_stream_opt.as_ref().unwrap().borrow_mut();
        let message_result = message_stream.next().await;
        match message_result {
            Some(message) => self.on_headers(message.headers),
            None => self.is_over.set(true),
        }
        debug!("FetchReceiveStage: done");
        Ok(())
    }

    pub fn can_proceed(&self) -> bool {
        let request_statuses = &[HeaderSliceStatus::Empty, HeaderSliceStatus::Waiting];
        let cant_receive_more =
            self.is_over.get() && self.header_slices.has_one_of_statuses(request_statuses);
        !cant_receive_more
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

        self.header_slices.find(
            start_block_num,
            move |slice_lock_opt: Option<&RwLock<HeaderSlice>>| {
                self.update_slice(slice_lock_opt, headers, start_block_num);
            },
        );
    }

    fn update_slice(
        &self,
        slice_lock_opt: Option<&RwLock<HeaderSlice>>,
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
                        warn!("FetchReceiveStage ignores a headers slice that we didn't request starting at: {}; status = {:?}", start_block_num, unexpected_status);
                    }
                }
            }
            None => {
                warn!(
                    "FetchReceiveStage ignores a headers slice that we didn't request starting at: {}",
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
