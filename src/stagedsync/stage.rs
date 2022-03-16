use super::stages::StageId;
use crate::{kv::mdbx::*, models::*};
use async_trait::async_trait;
use auto_impl::auto_impl;
use std::{fmt::Debug, time::Instant};

#[derive(Clone, Copy, Debug)]
pub struct StageInput {
    pub restarted: bool,
    pub first_started_at: (Instant, Option<BlockNumber>),
    pub previous_stage: Option<(StageId, BlockNumber)>,
    pub stage_progress: Option<BlockNumber>,
}

#[derive(Clone, Copy, Debug)]
pub struct UnwindInput {
    pub stage_progress: BlockNumber,
    pub unwind_to: BlockNumber,
}

#[derive(Clone, Copy, Debug)]
pub struct PruningInput {
    pub prune_progress: Option<BlockNumber>,
    pub prune_to: BlockNumber,
}

#[derive(Debug, PartialEq)]
pub enum ExecOutput {
    Unwind {
        unwind_to: BlockNumber,
    },
    Progress {
        stage_progress: BlockNumber,
        done: bool,
    },
}

#[derive(Debug, PartialEq)]
pub struct UnwindOutput {
    pub stage_progress: BlockNumber,
}

#[async_trait]
#[auto_impl(&mut, Box)]
pub trait Stage<'db, E>: Send + Sync + Debug
where
    E: EnvironmentKind,
{
    /// ID of the sync stage. Should not be empty and should be unique. It is recommended to prefix it with reverse domain to avoid clashes (`com.example.my-stage`).
    fn id(&self) -> StageId;
    /// Called when the stage is executed. The main logic of the stage should be here.
    async fn execute<'tx>(
        &mut self,
        tx: &'tx mut MdbxTransaction<'db, RW, E>,
        input: StageInput,
    ) -> anyhow::Result<ExecOutput>
    where
        'db: 'tx;
    /// Called when the stage should be unwound. The unwind logic should be there.
    async fn unwind<'tx>(
        &mut self,
        tx: &'tx mut MdbxTransaction<'db, RW, E>,
        input: UnwindInput,
    ) -> anyhow::Result<UnwindOutput>
    where
        'db: 'tx;

    async fn prune<'tx>(
        &mut self,
        _tx: &'tx mut MdbxTransaction<'db, RW, E>,
        _input: PruningInput,
    ) -> anyhow::Result<()>
    where
        'db: 'tx,
    {
        Ok(())
    }
}
