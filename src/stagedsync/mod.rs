pub mod stage;
pub mod stages;
pub mod unwind;

use self::stage::{Stage, StageInput, StageLogger, UnwindInput};
use crate::{kv::traits::MutableKV, MutableTransaction};
use async_trait::async_trait;
use futures_core::Future;
use tracing::info;

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

        let mut unwind_to = None;
        'run_loop: loop {
            let mut tx = db.begin_mutable().await?;

            if let Some(to) = &mut unwind_to {
                for (stage_index, stage) in self.stages.iter().rev().enumerate() {
                    let stage_id = stage.id();
                    let logger = StageLogger::new(stage_index, num_stages, stage_id);

                    logger.info("Unwinding");

                    stage
                        .unwind(&mut tx, UnwindInput { unwind_to: *to })
                        .await?;

                    logger.info("Unwinding complete");
                }

                unwind_to = None;
                tx.commit().await?;
            } else {
                let mut previous_stage = None;
                let mut timings = vec![];
                for (stage_index, stage) in self.stages.iter().enumerate() {
                    let mut restarted = false;

                    let stage_id = stage.id();
                    let logger = StageLogger::new(stage_index, num_stages, stage_id);

                    let start_time = std::time::Instant::now();
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

                        match exec_output {
                            stage::ExecOutput::Unwind { unwind_to: to } => {
                                unwind_to = Some(to);
                                continue 'run_loop;
                            }
                            stage::ExecOutput::Progress {
                                stage_progress,
                                done,
                            } => {
                                stage_id.save_progress(&tx, stage_progress).await?;

                                if done {
                                    logger.info("DONE");
                                    break stage_progress;
                                }

                                restarted = true
                            }
                        }
                    };
                    timings.push((stage_id, std::time::Instant::now() - start_time));

                    previous_stage = Some((stage_id, done_progress))
                }
                tx.commit().await?;

                let t = timings
                    .into_iter()
                    .fold(String::new(), |acc, (stage_id, time)| {
                        format!("{} {}={}ms", acc, stage_id, time.as_millis())
                    });
                info!("Staged sync complete.{}", t);

                self.sync_activator.wait().await;
            }
        }
    }
}
