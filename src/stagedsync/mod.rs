pub mod stage;
pub mod stages;

use self::stage::{Stage, StageInput, UnwindInput};
use crate::{kv::traits::*, stagedsync::stage::ExecOutput};
use std::time::{Duration, Instant};
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
    min_progress_to_commit_after_stage: u64,
}

impl<'db, DB: MutableKV> Default for StagedSync<'db, DB> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'db, DB: MutableKV> StagedSync<'db, DB> {
    pub fn new() -> Self {
        Self {
            stages: Vec::new(),
            min_progress_to_commit_after_stage: 0,
        }
    }

    pub fn push<S>(&mut self, stage: S)
    where
        S: Stage<'db, DB::MutableTx<'db>> + 'static,
    {
        self.stages.push(Box::new(stage))
    }

    pub fn set_min_progress_to_commit_after_stage(&mut self, v: u64) -> &mut Self {
        self.min_progress_to_commit_after_stage = v;
        self
    }

    /// Run staged sync loop.
    /// Invokes each loaded stage, and does unwinds if necessary.
    ///
    /// NOTE: it should never return, except if the loop or any stage fails with error.
    pub async fn run(&mut self, db: &'db DB) -> anyhow::Result<!> {
        let num_stages = self.stages.len();

        let mut unwind_to = None;
        'run_loop: loop {
            let mut tx = db.begin_mutable().await?;

            // Start with unwinding if it's been requested.
            if let Some(to) = unwind_to.take() {
                // Unwind stages in reverse order.
                for (stage_index, stage) in self.stages.iter_mut().enumerate().rev() {
                    let stage_id = stage.id();

                    // Unwind magic happens here.
                    // Encapsulated into a future for tracing instrumentation.
                    let res: anyhow::Result<()> = async {
                        let mut stage_progress =
                            stage_id.get_progress(&tx).await?.unwrap_or_default();

                        if stage_progress > to {
                            info!("UNWINDING from {}", stage_progress);

                            while stage_progress > to {
                                let unwind_output = stage
                                    .unwind(
                                        &mut tx,
                                        UnwindInput {
                                            stage_progress,
                                            unwind_to: to,
                                        },
                                    )
                                    .await?;

                                stage_progress = unwind_output.stage_progress;

                                stage_id.save_progress(&tx, stage_progress).await?;
                            }

                            info!("DONE @ {}", stage_progress);
                        } else {
                            debug!(
                                unwind_point = *to,
                                progress = *stage_progress,
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
                // Now that we're done with unwind, let's roll.

                let mut previous_stage = None;
                let mut timings = vec![];

                // Execute each stage in direct order.
                for (stage_index, stage) in self.stages.iter_mut().enumerate() {
                    let mut restarted = false;

                    let stage_id = stage.id();

                    let start_time = Instant::now();
                    let start_progress = stage_id.get_progress(&tx).await?;

                    // Re-invoke the stage until it reports `StageOutput::done`.
                    let done_progress = loop {
                        let prev_progress = stage_id.get_progress(&tx).await?;

                        let stage_id = stage.id();

                        let exec_output: anyhow::Result<_> = async {
                            if restarted {
                                debug!(
                                    "Invoking stage @ {}",
                                    prev_progress
                                        .map(|s| s.to_string())
                                        .unwrap_or_else(|| "genesis".to_string())
                                );
                            } else {
                                info!(
                                    "RUNNING from {}",
                                    prev_progress
                                        .map(|s| s.to_string())
                                        .unwrap_or_else(|| "genesis".to_string())
                                );
                            }

                            let invocation_start_time = Instant::now();
                            let output = stage
                                .execute(
                                    &mut tx,
                                    StageInput {
                                        restarted,
                                        first_started_at: (start_time, start_progress),
                                        previous_stage,
                                        stage_progress: prev_progress,
                                    },
                                )
                                .await?;

                            // Nothing here, pass along.
                            match &output {
                                ExecOutput::Progress {
                                    done,
                                    stage_progress,
                                    ..
                                } => {
                                    if *done {
                                        info!(
                                            "DONE @ {} in {}",
                                            stage_progress,
                                            format_duration(Instant::now() - start_time, true)
                                        );
                                    } else {
                                        debug!(
                                            "Stage invocation complete @ {}{} in {}",
                                            stage_progress,
                                            if let Some(prev_progress) = prev_progress {
                                                format!(
                                                    " (+{} blocks)",
                                                    stage_progress.saturating_sub(*prev_progress)
                                                )
                                            } else {
                                                String::new()
                                            },
                                            format_duration(
                                                Instant::now() - invocation_start_time,
                                                true
                                            )
                                        );
                                    }
                                }
                                ExecOutput::Unwind { unwind_to } => {
                                    info!(to = unwind_to.0, "Unwind requested");
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
                            AsRef::<str>::as_ref(&stage_id)
                        ))
                        .await;

                        // Check how stage run went.
                        match exec_output? {
                            stage::ExecOutput::Progress {
                                stage_progress,
                                done,
                            } => {
                                stage_id.save_progress(&tx, stage_progress).await?;

                                // Check if we should commit now.
                                if stage_progress
                                    .saturating_sub(start_progress.map(|v| v.0).unwrap_or(0))
                                    >= self.min_progress_to_commit_after_stage
                                {
                                    // Commit and restart transaction.
                                    debug!("Commit requested");
                                    tx.commit().await?;
                                    debug!("Commit complete");
                                    tx = db.begin_mutable().await?;
                                }

                                // Stage is "done", that is cannot make any more progress at this time.
                                if done {
                                    // Break out and move to the next stage.
                                    break stage_progress;
                                }

                                restarted = true
                            }
                            stage::ExecOutput::Unwind { unwind_to: to } => {
                                // Stage has asked us to unwind.
                                // Set unwind point and restart the whole staged sync loop.
                                // Current DB transaction will be aborted.
                                unwind_to = Some(to);
                                continue 'run_loop;
                            }
                        }
                    };
                    timings.push((stage_id, Instant::now() - start_time));

                    previous_stage = Some((stage_id, done_progress))
                }
                tx.commit().await?;

                let t = timings
                    .into_iter()
                    .fold(String::new(), |acc, (stage_id, time)| {
                        format!("{} {}={}", acc, stage_id, format_duration(time, true))
                    });
                info!("Staged sync complete.{}", t);
            }
        }
    }
}

pub fn format_duration(dur: Duration, subsec_millis: bool) -> String {
    let mut secs = dur.as_secs();
    let mut minutes = secs / 60;
    let hours = minutes / 60;

    secs %= 60;
    minutes %= 60;
    format!(
        "{:0>2}:{:0>2}:{:0>2}{}",
        hours,
        minutes,
        secs,
        if subsec_millis {
            format!(".{:0>3}", dur.subsec_millis())
        } else {
            String::new()
        }
    )
}
