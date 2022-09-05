use super::stages::StageId;
use crate::{consensus::ValidationError, kv::mdbx::*, models::*};
use async_trait::async_trait;
use auto_impl::auto_impl;
use std::{fmt::Debug, time::Instant};
use crate::consensus::DuoError;
use crate::stagedsync::stage::StageError::Validation;

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
    pub bad_block: Option<BlockNumber>,
}

#[derive(Clone, Copy, Debug)]
pub struct PruningInput {
    pub prune_progress: Option<BlockNumber>,
    pub prune_to: BlockNumber,
}

#[derive(Debug, PartialEq, Eq)]
pub enum ExecOutput {
    Unwind {
        unwind_to: BlockNumber,
    },
    Progress {
        stage_progress: BlockNumber,
        done: bool,
        reached_tip: bool,
    },
}

#[derive(Debug, PartialEq, Eq)]
pub struct UnwindOutput {
    pub stage_progress: BlockNumber,
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum StageError {
    Validation {
        block: BlockNumber,
        error: ValidationError,
    },
    Internal(anyhow::Error),
}

impl From<anyhow::Error> for StageError {
    fn from(e: anyhow::Error) -> Self {
        StageError::Internal(e)
    }
}

impl From<DuoError> for StageError {
    fn from(e: DuoError) -> Self {
        match e {
            DuoError::Validation(inner) => {
                StageError::Validation{
                    block: Default::default(),
                    error: inner,
                }
            },
            DuoError::Internal(inner) => {
                StageError::Internal(inner)
            }
        }
    }
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
    ) -> Result<ExecOutput, StageError>
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
