use super::stages::StageId;
use crate::MutableTransaction;
use async_trait::async_trait;
use auto_impl::auto_impl;
use std::fmt::Debug;

#[derive(Debug, PartialEq)]
pub enum ExecOutput {
    Unwind {
        unwind_to: u64,
    },
    Progress {
        stage_progress: u64,
        done: bool,
        must_commit: bool,
    },
}

#[async_trait]
#[auto_impl(&, Box, Arc)]
pub trait Stage<'db, RwTx: MutableTransaction<'db>>: Send + Sync + Debug {
    /// ID of the sync stage. Should not be empty and should be unique. It is recommended to prefix it with reverse domain to avoid clashes (`com.example.my-stage`).
    fn id(&self) -> StageId;
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
pub struct StageInput {
    pub restarted: bool,
    pub previous_stage: Option<(StageId, u64)>,
    pub stage_progress: Option<u64>,
}

#[derive(Clone, Copy, Debug)]
pub struct UnwindInput {
    pub stage_progress: u64,
    pub unwind_to: u64,
}
