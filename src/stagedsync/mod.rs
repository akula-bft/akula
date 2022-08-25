pub mod stage;
pub mod util;

use self::stage::{Stage, StageInput, UnwindInput};
use crate::{
    kv::mdbx::*,
    mining::{state::MiningConfig, StagedMining},
    models::*,
    stagedsync::stage::*,
    stages::CreateBlock,
    StageId,
};
use futures::future::BoxFuture;
use std::time::{Duration, Instant};
use tokio::sync::watch::{Receiver as WatchReceiver, Sender as WatchSender};
use tracing::*;

struct QueuedStage<'db, E>
where
    E: EnvironmentKind,
{
    stage: Box<dyn Stage<'db, E>>,
    unwind_priority: usize,
    require_tip: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StageExecutionReceipt {
    pub stage_id: StageId,
    pub progress: BlockNumber,
    pub duration: Duration,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StagedSyncStatus {
    pub maximum_progress: Option<BlockNumber>,
    pub minimum_progress: Option<BlockNumber>,
    pub stage_execution_receipts: Vec<StageExecutionReceipt>,
}

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
pub struct StagedSync<'db, E>
where
    E: EnvironmentKind,
{
    stages: Vec<QueuedStage<'db, E>>,
    current_stage_sender: WatchSender<Option<StageId>>,
    current_stage_receiver: WatchReceiver<Option<StageId>>,
    min_progress_to_commit_after_stage: u64,
    pruning_interval: u64,
    max_block: Option<BlockNumber>,
    start_with_unwind: Option<BlockNumber>,
    exit_after_sync: bool,
    delay_after_sync: Option<Duration>,
    post_cycle_callback:
        Option<Box<dyn Fn(StagedSyncStatus) -> BoxFuture<'static, ()> + Send + 'static>>,
    staged_mining: Option<StagedMining<'db, E>>,
}

impl<'db, E> Default for StagedSync<'db, E>
where
    E: EnvironmentKind,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<'db, E> StagedSync<'db, E>
where
    E: EnvironmentKind,
{
    pub fn new() -> Self {
        let (current_stage_sender, current_stage_receiver) = tokio::sync::watch::channel(None);
        Self {
            stages: Vec::new(),
            current_stage_sender,
            current_stage_receiver,
            min_progress_to_commit_after_stage: 0,
            pruning_interval: 0,
            max_block: None,
            start_with_unwind: None,
            exit_after_sync: false,
            delay_after_sync: None,
            post_cycle_callback: None,
            staged_mining: None,
        }
    }

    pub fn push<S>(&mut self, stage: S, require_tip: bool)
    where
        S: Stage<'db, E> + 'static,
    {
        self.stages.push(QueuedStage {
            stage: Box::new(stage),
            require_tip,
            unwind_priority: 0,
        })
    }

    pub fn push_with_unwind_priority<S>(
        &mut self,
        stage: S,
        require_tip: bool,
        unwind_priority: usize,
    ) where
        S: Stage<'db, E> + 'static,
    {
        self.stages.push(QueuedStage {
            stage: Box::new(stage),
            require_tip,
            unwind_priority,
        })
    }

    pub fn enable_mining(&mut self, config: MiningConfig) {
        let mut mining = StagedMining::new();
        mining.push(CreateBlock { config });
        // TODO other stages
        self.staged_mining = Some(mining);
    }

    pub fn set_pruning_interval(&mut self, v: u64) -> &mut Self {
        self.pruning_interval = v;
        self
    }

    pub fn set_min_progress_to_commit_after_stage(&mut self, v: u64) -> &mut Self {
        self.min_progress_to_commit_after_stage = v;
        self
    }

    pub fn set_max_block(&mut self, v: Option<BlockNumber>) -> &mut Self {
        self.max_block = v;
        self
    }

    pub fn start_with_unwind(&mut self, v: Option<BlockNumber>) -> &mut Self {
        self.start_with_unwind = v;
        self
    }

    pub fn set_post_cycle_callback(
        &mut self,
        f: impl Fn(StagedSyncStatus) -> BoxFuture<'static, ()> + Send + 'static,
    ) -> &mut Self {
        self.post_cycle_callback = Some(Box::new(f));
        self
    }

    pub fn set_exit_after_sync(&mut self, v: bool) -> &mut Self {
        self.exit_after_sync = v;
        self
    }

    pub fn set_delay_after_sync(&mut self, v: Option<Duration>) -> &mut Self {
        self.delay_after_sync = v;
        self
    }

    pub fn current_stage(&self) -> WatchReceiver<Option<StageId>> {
        self.current_stage_receiver.clone()
    }

    /// Run staged sync loop.
    /// Invokes each loaded stage, and does unwinds if necessary.
    ///
    /// NOTE: it should never return, except if the loop or any stage fails with error.
    pub async fn run(&mut self, db: &'db MdbxEnvironment<E>) -> anyhow::Result<()> {
        let num_stages = self.stages.len();

        let mut bad_block = None;
        let mut unwind_to = self.start_with_unwind;
        'run_loop: loop {
            self.current_stage_sender.send(None).unwrap();

            let mut tx = db.begin_mutable()?;

            // Start with unwinding if it's been requested.
            if let Some(to) = unwind_to.take() {
                let mut unwind_pipeline = self.stages.iter_mut().enumerate().collect::<Vec<_>>();

                unwind_pipeline.sort_by_key(|(idx, stage)| {
                    if stage.unwind_priority > 0 {
                        (idx - stage.unwind_priority, 0)
                    } else {
                        (*idx, 1)
                    }
                });

                // Unwind stages in reverse order.
                for (stage_index, QueuedStage { stage, .. }) in unwind_pipeline.into_iter().rev() {
                    let stage_id = stage.id();

                    // Unwind magic happens here.
                    // Encapsulated into a future for tracing instrumentation.
                    let res: anyhow::Result<()> = async {
                        let mut stage_progress = stage_id.get_progress(&tx)?.unwrap_or_default();

                        if stage_progress > to {
                            info!("UNWINDING from {}", stage_progress);

                            while stage_progress > to {
                                let unwind_output = stage
                                    .unwind(
                                        &mut tx,
                                        UnwindInput {
                                            stage_progress,
                                            unwind_to: to,
                                            bad_block,
                                        },
                                    )
                                    .await?;

                                stage_progress = unwind_output.stage_progress;

                                stage_id.save_progress(&tx, stage_progress)?;
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

                bad_block = None;
                tx.commit()?;
            } else {
                // Now that we're done with unwind, let's roll.

                let mut previous_stage = None;
                let mut receipts = Vec::with_capacity(num_stages);

                let mut minimum_progress = None;
                let mut maximum_progress = None;

                let mut reached_tip_flag = true;

                // Execute each stage in direct order.
                for (
                    stage_index,
                    QueuedStage {
                        stage, require_tip, ..
                    },
                ) in self.stages.iter_mut().enumerate()
                {
                    let mut restarted = false;

                    let stage_id = stage.id();

                    self.current_stage_sender.send(Some(stage.id())).unwrap();

                    let start_time = Instant::now();
                    let start_progress = stage_id.get_progress(&tx)?;
                    // Re-invoke the stage until it reports `StageOutput::done`.
                    let done_progress = loop {
                        let prev_progress = stage_id.get_progress(&tx)?;

                        let stage_id = stage.id();

                        let exec_output: Result<_, StageError> = async {
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

                            let output = if !reached_tip_flag
                                && *require_tip
                                && maximum_progress
                                    .map(|maximum_progress| {
                                        maximum_progress
                                            < self.max_block.unwrap_or(BlockNumber(u64::MAX))
                                    })
                                    .unwrap_or(true)
                            {
                                info!("Tip not reached, skipping stage");

                                ExecOutput::Progress {
                                    stage_progress: prev_progress.unwrap_or_default(),
                                    done: true,
                                    reached_tip: false,
                                }
                            } else if prev_progress.unwrap_or(BlockNumber(0))
                                >= self.max_block.unwrap_or_else(|| u64::MAX.into())
                            {
                                info!("Reached max block, skipping stage");

                                ExecOutput::Progress {
                                    stage_progress: prev_progress.unwrap_or_default(),
                                    done: true,
                                    reached_tip: true,
                                }
                            } else {
                                stage
                                    .execute(
                                        &mut tx,
                                        StageInput {
                                            restarted,
                                            first_started_at: (start_time, start_progress),
                                            previous_stage,
                                            stage_progress: prev_progress,
                                        },
                                    )
                                    .await?
                            };

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
                        match exec_output {
                            Ok(stage::ExecOutput::Progress {
                                stage_progress,
                                done,
                                reached_tip,
                            }) => {
                                stage_id.save_progress(&tx, stage_progress)?;

                                macro_rules! record_outliers {
                                    ($f:expr, $v:expr) => {
                                        if let Some(m) = $v {
                                            *m = $f(*m, stage_progress);
                                        } else {
                                            *$v = Some(stage_progress);
                                        }
                                    };
                                }

                                record_outliers!(std::cmp::min, &mut minimum_progress);
                                record_outliers!(std::cmp::max, &mut maximum_progress);

                                // Check if we should commit now.
                                if stage_progress
                                    .saturating_sub(start_progress.map(|v| v.0).unwrap_or(0))
                                    >= self.min_progress_to_commit_after_stage
                                {
                                    // Commit and restart transaction.
                                    debug!("Commit requested");
                                    tx.commit()?;
                                    debug!("Commit complete");
                                    tx = db.begin_mutable()?;
                                }

                                // Stage is "done", that is cannot make any more progress at this time.
                                if done {
                                    if !reached_tip {
                                        reached_tip_flag = false;
                                    }

                                    // Break out and move to the next stage.
                                    break stage_progress;
                                }

                                restarted = true
                            }
                            Ok(stage::ExecOutput::Unwind { unwind_to: to }) => {
                                // Stage has asked us to unwind.
                                // Set unwind point and restart the whole staged sync loop.
                                // Current DB transaction will be aborted.
                                unwind_to = Some(to);
                                continue 'run_loop;
                            }
                            Err(StageError::Validation { block, error }) => {
                                warn!("Block #{block}, failed validation: {error:?}");
                                bad_block = Some(block);
                                unwind_to = Some(prev_progress.unwrap_or_default());
                                continue 'run_loop;
                            }
                            Err(StageError::Internal(e)) => {
                                return Err(e);
                            }
                        }
                    };
                    receipts.push(StageExecutionReceipt {
                        stage_id,
                        progress: done_progress,
                        duration: Instant::now() - start_time,
                    });

                    previous_stage = Some((stage_id, done_progress))
                }

                self.current_stage_sender.send(None).unwrap();

                let t = receipts.iter().fold(
                    String::new(),
                    |acc,
                     StageExecutionReceipt {
                         stage_id, duration, ..
                     }| {
                        format!("{} {}={}", acc, stage_id, format_duration(*duration, true))
                    },
                );
                info!("Staged sync complete.{}", t);

                if let Some(minimum_progress) = minimum_progress {
                    if self.pruning_interval > 0 {
                        if let Some(prune_to) =
                            minimum_progress.0.checked_sub(self.pruning_interval)
                        {
                            let prune_to = BlockNumber(prune_to);

                            // Prune all stages
                            for (stage_index, QueuedStage { stage, .. }) in
                                self.stages.iter_mut().enumerate().rev()
                            {
                                let stage_id = stage.id();

                                let span = span!(
                                    Level::INFO,
                                    "",
                                    " Pruning {}/{} {} ",
                                    stage_index + 1,
                                    num_stages,
                                    AsRef::<str>::as_ref(&stage_id)
                                );
                                let _g = span.enter();

                                let prune_progress = stage_id.get_prune_progress(&tx)?;

                                if let Some(prune_progress) = prune_progress {
                                    if prune_progress >= prune_to {
                                        debug!(
                                            prune_to = *prune_to,
                                            progress = *prune_progress,
                                            "No pruning required"
                                        );

                                        continue;
                                    }
                                }

                                info!(
                                    "PRUNING from {}",
                                    prune_progress
                                        .map(|s| s.to_string())
                                        .unwrap_or_else(|| "genesis".to_string())
                                );

                                stage
                                    .prune(
                                        &mut tx,
                                        PruningInput {
                                            prune_progress,
                                            prune_to,
                                        },
                                    )
                                    .await?;

                                stage_id.save_prune_progress(&tx, prune_to)?;

                                info!("PRUNED to {}", prune_to);
                            }
                        }
                    }
                }

                tx.commit()?;

                let last_block = receipts.last().map_or(BlockNumber(0), |r| r.progress);

                if let Some(cb) = &self.post_cycle_callback {
                    (cb)(StagedSyncStatus {
                        maximum_progress,
                        minimum_progress,
                        stage_execution_receipts: receipts,
                    })
                    .await
                }

                if let Some(mining) = &mut self.staged_mining {
                    let mut tx = db.begin_mutable()?;
                    async {
                        mining.run(&mut tx, last_block).await;
                    }
                    .await;
                }

                if let Some(minimum_progress) = minimum_progress {
                    if let Some(max_block) = self.max_block {
                        if minimum_progress >= max_block {
                            return Ok(());
                        }
                    }
                }

                if let Some(delay_after_sync) = self.delay_after_sync {
                    tokio::time::sleep(delay_after_sync).await
                }
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
