use crate::{
    downloader::{opts::Opts, Downloader},
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

        self.downloader.run(None, tx).await?;

        Ok(ExecOutput::Progress {
            stage_progress: past_progress,
            done: false,
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
