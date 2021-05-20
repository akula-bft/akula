pub mod stage;
pub mod stage_factory;
pub mod stages;
pub mod state;
pub mod unwind;

use std::sync::Arc;

use self::stage::{Stage, StageInput, StageLogger};
use crate::{
    kv::traits::{MutableKV, KV},
    MutableTransaction, SyncStage,
};
use async_trait::async_trait;
use futures_core::Future;

// use self::stage_factory::StageFactory;

#[async_trait]
pub trait SyncActivator: 'static {
    async fn wait(&self);
}

#[async_trait]
impl<F: Fn() -> Fut + Send + Sync + 'static, Fut: Future<Output = ()> + Send> SyncActivator for F {
    async fn wait(&self) {
        (self)().await
    }
}

pub struct StagedSync<'db, DB: MutableKV> {
    stages: Vec<Box<dyn Stage<'db, DB::MutableTx<'db>>>>,
    sync_activator: Box<dyn SyncActivator>,
}

impl<'db, DB: MutableKV> StagedSync<'db, DB> {
    pub fn new<A>(sync_activator: A) -> Self
    where
        A: SyncActivator,
    {
        Self {
            stages: Vec::new(),
            sync_activator: Box::new(sync_activator),
        }
    }

    pub fn push<S>(&mut self, stage: S)
    where
        S: Stage<'db, DB::MutableTx<'db>> + 'static,
    {
        self.stages.push(Box::new(stage))
    }

    pub async fn run(&self, db: &'db DB) -> anyhow::Result<!> {
        let num_stages = self.stages.len();
        loop {
            let mut tx = db.begin_mutable().await?;

            let mut previous_stage = None;
            for (stage_index, stage) in self.stages.iter().enumerate() {
                let mut restarted = false;

                let stage_id = stage.id();
                let logger = StageLogger::new(stage_index, num_stages, stage_id);

                let done_progress = loop {
                    let stage_progress = stage_id.get_progress(&tx).await?;

                    if !restarted {
                        logger.info("RUNNING");
                    }
                    let exec_output = stage
                        .execute(
                            &mut tx,
                            StageInput {
                                restarted,
                                previous_stage,
                                stage_progress,
                                logger,
                            },
                        )
                        .await?;

                    stage_id
                        .save_progress(&tx, exec_output.stage_progress)
                        .await?;

                    if exec_output.done {
                        logger.info("DONE");
                        break exec_output.stage_progress;
                    }

                    restarted = true
                };

                previous_stage = Some((stage_id, done_progress))
            }

            tx.commit().await?;

            self.sync_activator.wait().await;
        }
    }
}
