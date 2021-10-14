use crate::{
    downloader::{
        chain_config::{ChainConfig, ChainsConfig},
        headers::{
            fetch_receive_stage::FetchReceiveStage, fetch_request_stage::FetchRequestStage,
            header_slices, header_slices::HeaderSlices,
            preverified_hashes_config::PreverifiedHashesConfig, refill_stage::RefillStage,
            retry_stage::RetryStage, save_stage::SaveStage, verify_stage::VerifyStage,
            HeaderSlicesView,
        },
        opts::Opts,
        sentry_client,
        sentry_client::SentryClient,
        sentry_client_impl::SentryClientImpl,
        sentry_client_reactor::SentryClientReactor,
    },
    kv,
    models::BlockNumber,
};
use futures_core::Stream;
use parking_lot::RwLock;
use std::{pin::Pin, sync::Arc};
use tokio_stream::{StreamExt, StreamMap};
use tracing::*;

type StageStream = Pin<Box<dyn Stream<Item = anyhow::Result<()>>>>;

pub struct Downloader<DB: kv::traits::MutableKV> {
    opts: Opts,
    chain_config: ChainConfig,
    db: Arc<DB>,
}

impl<DB: kv::traits::MutableKV> Downloader<DB> {
    pub fn new(opts: Opts, chains_config: ChainsConfig, db: Arc<DB>) -> Self {
        let chain_config = chains_config.0[&opts.chain_name].clone();

        Self {
            opts,
            chain_config,
            db,
        }
    }

    pub async fn run(
        &self,
        sentry_client_opt: Option<Box<dyn SentryClient>>,
    ) -> anyhow::Result<()> {
        let status = sentry_client::Status {
            total_difficulty: ethereum_types::U256::zero(),
            best_hash: ethereum_types::H256::zero(),
            chain_fork_config: self.chain_config.clone(),
            max_block: 0,
        };

        let mut sentry_client = match sentry_client_opt {
            Some(v) => v,
            None => Box::new(SentryClientImpl::new(self.opts.sentry_api_addr.clone()).await?),
        };

        sentry_client.set_status(status).await?;

        let mut sentry_reactor = SentryClientReactor::new(sentry_client);
        sentry_reactor.start()?;

        let mut ui_system = crate::downloader::ui_system::UISystem::new();
        ui_system.start()?;

        let preverified_hashes_config = PreverifiedHashesConfig::new(&self.opts.chain_name)?;

        let header_slices_mem_limit = 50 << 20; /* 50 Mb */
        let header_slices_final_block_num = BlockNumber(
            ((preverified_hashes_config.hashes.len() - 1) * header_slices::HEADER_SLICE_SIZE)
                as u64,
        );
        let header_slices = Arc::new(HeaderSlices::new(
            header_slices_mem_limit,
            header_slices_final_block_num,
        ));
        let sentry = Arc::new(RwLock::new(sentry_reactor));

        ui_system.set_view(Some(Box::new(HeaderSlicesView::new(header_slices.clone()))));

        // Downloading happens with several stages where
        // each of the stages processes blocks in one status,
        // and updates them to proceed to the next status.
        // All stages runs in parallel,
        // although most of the time only one of the stages is actively running,
        // while the others are waiting for the status updates or timeouts.

        let mut fetch_request_stage = FetchRequestStage::new(header_slices.clone(), sentry.clone());
        let mut fetch_receive_stage = FetchReceiveStage::new(header_slices.clone(), sentry.clone());
        let mut retry_stage = RetryStage::new(header_slices.clone());
        let mut verify_stage = VerifyStage::new(header_slices.clone(), preverified_hashes_config);
        let mut save_stage = SaveStage::new(header_slices.clone(), self.db.clone());
        let mut refill_stage = RefillStage::new(header_slices.clone());

        let can_proceed = fetch_receive_stage.can_proceed_checker();

        let fetch_request_stage_stream: StageStream = Box::pin(async_stream::stream! {
            loop {
                yield fetch_request_stage.execute().await;
            }
        });
        let fetch_receive_stage_stream: StageStream = Box::pin(async_stream::stream! {
            loop {
                yield fetch_receive_stage.execute().await;
            }
        });
        let retry_stage_stage_stream: StageStream = Box::pin(async_stream::stream! {
            loop {
                yield retry_stage.execute().await;
            }
        });
        let verify_stage_stage_stream: StageStream = Box::pin(async_stream::stream! {
            loop {
                yield verify_stage.execute().await;
            }
        });
        let save_stage_stage_stream: StageStream = Box::pin(async_stream::stream! {
            loop {
                yield save_stage.execute().await;
            }
        });
        let refill_stage_stage_stream: StageStream = Box::pin(async_stream::stream! {
            loop {
                yield refill_stage.execute().await;
            }
        });

        let mut stream = StreamMap::<&str, StageStream>::new();
        stream.insert("fetch_request_stage_stream", fetch_request_stage_stream);
        stream.insert("fetch_receive_stage_stream", fetch_receive_stage_stream);
        stream.insert("retry_stage_stage_stream", retry_stage_stage_stream);
        stream.insert("verify_stage_stage_stream", verify_stage_stage_stream);
        stream.insert("save_stage_stage_stream", save_stage_stage_stream);
        stream.insert("refill_stage_stage_stream", refill_stage_stage_stream);

        while let Some((key, result)) = stream.next().await {
            if result.is_err() {
                error!("Downloader headers {} failure: {:?}", key, result);
                break;
            }

            if !can_proceed.can_proceed() {
                break;
            }
            if header_slices.is_empty_at_final_position() {
                break;
            }

            header_slices.notify_status_watchers();
        }

        ui_system.stop().await?;

        {
            let mut sentry_reactor = sentry.write();
            sentry_reactor.stop().await?;
        }

        Ok(())
    }
}
