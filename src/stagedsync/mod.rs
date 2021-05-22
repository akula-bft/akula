pub mod stage;
pub mod stages;

use self::stage::{Stage, StageInput, UnwindInput};
use crate::{kv::traits::MutableKV, stagedsync::stage::ExecOutput, MutableTransaction};
use tracing::*;

/// Staged synchronization framework
///
/// As the name suggests, the gist of this framework is splitting sync into logical _stages_ that are consecutively executed one after another.
/// It is I/O intensive and even though we have a goal on being able to sync the node on an HDD, we still recommend using fast SSDs.
///
/// # How it works
/// For each peer we learn what the HEAD block is and it executes each stage in order for the missing blocks between the local HEAD block and the peer's head block.
/// The first stage (downloading headers) sets the local HEAD block.
/// Each stage is executed in order and a stage N does not stop until the local head is reached for it.
/// That means, that in the ideal scenario (no network interruptions, the app isn't restarted, etc), for the full initial sync, each stage will be executed exactly once.
/// After the last stage is finished, the process starts from the beginning, by looking for the new headers to download.
/// If the app is restarted in between stages, it restarts from the first stage. Absent new blocks, already completed stages are skipped.
pub struct StagedSync<'db, DB: MutableKV> {
    stages: Vec<Box<dyn Stage<'db, DB::MutableTx<'db>>>>,
}

impl<'db, DB: MutableKV> Default for StagedSync<'db, DB> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'db, DB: MutableKV> StagedSync<'db, DB> {
    pub fn new() -> Self {
        Self { stages: Vec::new() }
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
            let mut tx = Some(db.begin_mutable().await?);

            if let Some(to) = unwind_to.take() {
                let mut tx = tx.unwrap();
                for (stage_index, stage) in self.stages.iter().rev().enumerate() {
                    let stage_id = stage.id();

                    let res: anyhow::Result<()> = async {
                        let stage_progress = stage_id.get_progress(&tx).await?.unwrap_or(0);

                        if stage_progress > to {
                            info!("RUNNING");

                            stage
                                .unwind(
                                    &mut tx,
                                    UnwindInput {
                                        stage_progress,
                                        unwind_to: to,
                                    },
                                )
                                .await?;

                            stage_id.save_progress(&tx, to).await?;

                            info!("DONE");
                        } else {
                            debug!(
                                unwind_point = to,
                                progress = stage_progress,
                                "Unwind point too far for stage"
                            );
                        }

                        Ok(())
                    }
                    .instrument(span!(
                        Level::INFO,
                        "",
                        " Unwinding {}/{} {} ",
                        stage_index + 1,
                        num_stages,
                        AsRef::<str>::as_ref(&stage_id)
                    ))
                    .await;

                    res?;
                }

                tx.commit().await?;
            } else {
                let mut previous_stage = None;
                let mut timings = vec![];
                for (stage_index, stage) in self.stages.iter().enumerate() {
                    let mut restarted = false;

                    let stage_id = stage.id();

                    let start_time = std::time::Instant::now();
                    let done_progress = loop {
                        let mut t = tx.take().unwrap();

                        let stage_progress = stage_id.get_progress(&t).await?;

                        let exec_output: anyhow::Result<_> = async {
                            if !restarted {
                                info!("RUNNING");
                            }

                            let output = stage
                                .execute(
                                    &mut t,
                                    StageInput {
                                        restarted,
                                        previous_stage,
                                        stage_progress,
                                    },
                                )
                                .await?;

                            match &output {
                                ExecOutput::Progress { done, .. } => {
                                    if *done {
                                        info!("DONE");
                                    }
                                }
                                ExecOutput::Unwind { unwind_to } => {
                                    info!(to = unwind_to, "Unwind requested");
                                }
                            }

                            Ok(output)
                        }
                        .instrument(span!(
                            Level::INFO,
                            "",
                            " {}/{} {} ",
                            stage_index + 1,
                            num_stages,
                            AsRef::<str>::as_ref(&stage.id())
                        ))
                        .await;

                        match exec_output? {
                            stage::ExecOutput::Progress {
                                stage_progress,
                                done,
                                must_commit,
                            } => {
                                stage_id.save_progress(&t, stage_progress).await?;

                                if must_commit {
                                    t.commit().await?;
                                    tx = Some(db.begin_mutable().await?);
                                } else {
                                    // Return tx object back
                                    tx = Some(t);
                                }

                                if done {
                                    break stage_progress;
                                }

                                restarted = true
                            }
                            stage::ExecOutput::Unwind { unwind_to: to } => {
                                unwind_to = Some(to);
                                continue 'run_loop;
                            }
                        }
                    };
                    timings.push((stage_id, std::time::Instant::now() - start_time));

                    previous_stage = Some((stage_id, done_progress))
                }
                tx.unwrap().commit().await?;

                let t = timings
                    .into_iter()
                    .fold(String::new(), |acc, (stage_id, time)| {
                        format!("{} {}={}ms", acc, stage_id, time.as_millis())
                    });
                info!("Staged sync complete.{}", t);
            }
        }
    }
}
