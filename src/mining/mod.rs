use crate::{
    kv::mdbx::MdbxTransaction,
    models::BlockNumber,
    stagedsync::{
        format_duration,
        stage::{ExecOutput, Stage, StageInput},
    },
};
use mdbx::{EnvironmentKind, RW};
use std::time::Instant;
use tracing::*;

pub mod proposal;
pub mod state;

#[derive(Debug)]
pub struct StagedMining<'db, E>
where
    E: EnvironmentKind,
{
    stages: Vec<Box<dyn Stage<'db, E>>>,
}

impl<'db, E> Default for StagedMining<'db, E>
where
    E: EnvironmentKind,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<'db, E> StagedMining<'db, E>
where
    E: EnvironmentKind,
{
    pub fn new() -> Self {
        Self { stages: Vec::new() }
    }

    pub fn push<S>(&mut self, stage: S)
    where
        S: Stage<'db, E> + 'static,
    {
        self.stages.push(Box::new(stage));
    }

    pub async fn run<'tx>(
        &mut self,
        tx: &'tx mut MdbxTransaction<'db, RW, E>,
        last_block: BlockNumber,
    ) where
        'db: 'tx,
    {
        let num_stages = self.stages.len();

        for (stage_index, stage) in self.stages.iter_mut().enumerate() {
            let stage_started = Instant::now();
            let stage_id = stage.id().0;

            let success = async {
                info!("RUNNING");

                let input = StageInput {
                    restarted: false,
                    first_started_at: (stage_started, Some(last_block)),
                    previous_stage: None,
                    stage_progress: Some(last_block),
                };

                let output = stage.execute(tx, input).await;

                let new_block = last_block.0 + 1;

                let success = matches!(
                    &output,
                    Ok(ExecOutput::Progress { stage_progress, .. }) if stage_progress.0 == new_block
                );

                if success {
                    info!("DONE in {}", format_duration(stage_started.elapsed(), true));
                } else {
                    warn!(
                        "Creating a block proposal for height {} failed: {}",
                        new_block,
                        if let Err(e) = output {
                            format!("{:?}", e)
                        } else {
                            "".to_string()
                        },
                    );
                }

                success
            }
            .instrument(span!(
                Level::INFO,
                "",
                " {}/{} Mining {} ",
                stage_index + 1,
                num_stages,
                stage_id,
            ))
            .await;

            if !success {
                break;
            }
        }
    }
}
