use crate::{
    downloader::{opts::Opts, Downloader},
    models::BlockNumber,
    sentry::chain_config::ChainsConfig,
    stagedsync::stage::{ExecOutput, Stage, StageInput},
    MutableTransaction, StageId,
};
use async_trait::async_trait;

#[derive(Debug)]
pub struct HeaderDownload {
    downloader: Downloader,
}

impl HeaderDownload {
    pub fn new(opts: Opts, chains_config: ChainsConfig) -> anyhow::Result<Self> {
        let downloader = Downloader::new(opts, chains_config)?;

        Ok(Self { downloader })
    }
}

#[async_trait]
impl<'db, RwTx> Stage<'db, RwTx> for HeaderDownload
where
    RwTx: MutableTransaction<'db>,
{
    fn id(&self) -> StageId {
        StageId("HeaderDownload")
    }

    fn description(&self) -> &'static str {
        "Downloading headers"
    }

    async fn execute<'tx>(&self, tx: &'tx mut RwTx, input: StageInput) -> anyhow::Result<ExecOutput>
    where
        'db: 'tx,
    {
        let past_progress = input.stage_progress.unwrap_or_default();

        let start_block_num = BlockNumber(past_progress.0 + 1);
        let final_block_num = self.downloader.run(None, tx, start_block_num).await?;

        let stage_progress = if final_block_num.0 > 0 {
            BlockNumber(final_block_num.0 - 1)
        } else {
            past_progress
        };

        Ok(ExecOutput::Progress {
            stage_progress,
            done: true,
            must_commit: true,
        })
    }

    async fn unwind<'tx>(
        &self,
        tx: &'tx mut RwTx,
        input: crate::stagedsync::stage::UnwindInput,
    ) -> anyhow::Result<()>
    where
        'db: 'tx,
    {
        let _ = tx;
        let _ = input;
        todo!()
    }
}
