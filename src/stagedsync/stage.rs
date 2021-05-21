use std::fmt::Display;

use super::stages::SyncStage;
use crate::MutableTransaction;
use async_trait::async_trait;
use auto_impl::auto_impl;
use tracing::*;

pub enum ExecOutput {
    Unwind { unwind_to: u64 },
    Progress { stage_progress: u64, done: bool },
}

#[async_trait(?Send)]
#[auto_impl(&, Box, Arc)]
pub trait Stage<'db, RwTx: MutableTransaction<'db>> {
    /// ID of the sync stage. Should not be empty and should be unique. It is recommended to prefix it with reverse domain to avoid clashes (`com.example.my-stage`).
    fn id(&self) -> SyncStage;
    /// Description of the stage.
    fn description(&self) -> &'static str;
    /// Called when the stage is executed. The main logic of the stage should be here.
    async fn execute<'tx>(
        &self,
        tx: &'tx mut RwTx,
        input: StageInput,
    ) -> anyhow::Result<ExecOutput>
    where
        'db: 'tx;
    /// Called when the stage should be unwound. The unwind logic should be there.
    async fn unwind<'tx>(&self, tx: &'tx mut RwTx, input: UnwindInput) -> anyhow::Result<()>
    where
        'db: 'tx;
}

#[derive(Clone, Copy, Debug)]
pub struct StageLogger {
    stage_index: usize,
    num_stages: usize,
    stage: SyncStage,
}

impl StageLogger {
    pub fn new(stage_index: usize, num_stages: usize, stage: SyncStage) -> Self {
        Self {
            stage_index,
            num_stages,
            stage,
        }
    }

    fn msg<T: Display>(&self, msg: T) -> String {
        format!(
            "[ {}/{} {} ] {}",
            self.stage_index + 1,
            self.num_stages,
            self.stage,
            msg
        )
    }

    pub fn info<T: Display>(&self, msg: T) {
        let m = self.msg(msg);
        info!("{}", m)
    }

    pub fn debug<T: Display>(&self, msg: T) {
        let m = self.msg(msg);
        debug!("{}", m)
    }

    pub fn trace<T: Display>(&self, msg: T) {
        let m = self.msg(msg);
        trace!("{}", m)
    }

    pub fn warn<T: Display>(&self, msg: T) {
        let m = self.msg(msg);
        warn!("{}", m)
    }

    pub fn error<T: Display>(&self, msg: T) {
        let m = self.msg(msg);
        error!("{}", m)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct StageInput {
    pub restarted: bool,
    pub previous_stage: Option<(SyncStage, u64)>,
    pub stage_progress: Option<u64>,
    pub logger: StageLogger,
}

#[derive(Clone, Copy, Debug)]
pub struct UnwindInput {
    pub unwind_to: u64,
}
