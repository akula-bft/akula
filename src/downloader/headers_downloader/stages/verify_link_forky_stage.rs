use super::{
    fork_mode_stage::ForkModeStage,
    fork_switch_command::ForkSwitchCommand,
    headers::{
        header::BlockHeader,
        header_slices::{HeaderSliceStatus, HeaderSlices},
    },
    verification::header_slice_verifier::HeaderSliceVerifier,
    verify_link_linear_stage::VerifyLinkLinearStage,
};
use crate::{models::*, sentry::chain_config::ChainConfig};
use parking_lot::RwLock;
use std::{ops::DerefMut, sync::Arc};
use tracing::*;

/// Verifies the sequence rules to grow the slices chain and sets Verified status.
pub struct VerifyLinkForkyStage {
    header_slices: Arc<HeaderSlices>,
    fork_header_slices: Arc<HeaderSlices>,
    chain_config: ChainConfig,
    verifier: Arc<Box<dyn HeaderSliceVerifier>>,
    last_verified_header: Option<BlockHeader>,
    start_block_num: BlockNumber,
    mode: Mode,
    termination_command: Arc<RwLock<Option<ForkSwitchCommand>>>,
}

enum Mode {
    Linear(Box<VerifyLinkLinearStage>),
    Fork(Box<ForkModeStage>),
}

impl VerifyLinkForkyStage {
    pub fn new(
        header_slices: Arc<HeaderSlices>,
        fork_header_slices: Arc<HeaderSlices>,
        chain_config: ChainConfig,
        verifier: Arc<Box<dyn HeaderSliceVerifier>>,
        last_verified_header: Option<BlockHeader>,
    ) -> Self {
        let mode = if fork_header_slices.is_empty() {
            let linear_mode_stage = VerifyLinkLinearStage::new(
                header_slices.clone(),
                chain_config.clone(),
                verifier.clone(),
                last_verified_header.clone(),
                HeaderSliceStatus::Fork,
            );
            Mode::Linear(Box::new(linear_mode_stage))
        } else {
            let fork_mode_stage = ForkModeStage::new(
                header_slices.clone(),
                fork_header_slices.clone(),
                chain_config.clone(),
                verifier.clone(),
            );
            Mode::Fork(Box::new(fork_mode_stage))
        };

        let start_block_num = if let Some(last_verified_header) = &last_verified_header {
            BlockNumber(last_verified_header.number().0 + 1)
        } else {
            BlockNumber(0)
        };

        Self {
            header_slices,
            fork_header_slices,
            chain_config,
            verifier,
            last_verified_header,
            start_block_num,
            mode,
            termination_command: Arc::new(RwLock::new(None)),
        }
    }

    pub async fn execute(&mut self) -> anyhow::Result<()> {
        // execute sub-stage
        let sub_stage: &mut dyn super::stage::Stage = match self.mode {
            Mode::Linear(ref mut stage) => {
                debug!("VerifyLinkForkyStage: Mode::Linear");
                stage.as_mut()
            }
            Mode::Fork(ref mut stage) => {
                debug!("VerifyLinkForkyStage: Mode::Fork");
                stage.as_mut()
            }
        };
        sub_stage.execute().await?;

        // check mode switch conditions
        match self.mode {
            Mode::Linear(_) => {
                if let Some(fork_slice_lock) =
                    self.header_slices.find_by_status(HeaderSliceStatus::Fork)
                {
                    // do not fork from the start_block_num
                    if fork_slice_lock.read().start_block_num == self.start_block_num {
                        let mut fork_slice = fork_slice_lock.write();
                        self.header_slices
                            .set_slice_status(fork_slice.deref_mut(), HeaderSliceStatus::Invalid);
                    } else {
                        debug!("VerifyLinkForkyStage: switching to Mode::Fork");
                        self.switch_to_fork_mode()?;
                    }
                }
            }
            Mode::Fork(ref mut stage) => {
                if stage.is_over() {
                    debug!("VerifyLinkForkyStage: ForkModeStage is over, request termination");
                    *self.termination_command.write() = stage.take_pending_termination_command();
                } else if stage.is_done() {
                    debug!("VerifyLinkForkyStage: switching to Mode::Linear");
                    self.switch_to_linear_mode();
                }
            }
        }

        Ok(())
    }

    fn make_linear_mode_stage(&self) -> VerifyLinkLinearStage {
        VerifyLinkLinearStage::new(
            self.header_slices.clone(),
            self.chain_config.clone(),
            self.verifier.clone(),
            self.last_verified_header.clone(),
            HeaderSliceStatus::Fork,
        )
    }

    fn switch_to_linear_mode(&mut self) {
        self.mode = Mode::Linear(Box::new(self.make_linear_mode_stage()));
    }

    fn make_fork_mode_stage(&self) -> ForkModeStage {
        ForkModeStage::new(
            self.header_slices.clone(),
            self.fork_header_slices.clone(),
            self.chain_config.clone(),
            self.verifier.clone(),
        )
    }

    fn switch_to_fork_mode(&mut self) -> anyhow::Result<()> {
        let mut fork_mode_stage = self.make_fork_mode_stage();
        fork_mode_stage.setup()?;
        self.mode = Mode::Fork(Box::new(fork_mode_stage));
        Ok(())
    }

    pub fn termination_command(&self) -> Arc<RwLock<Option<ForkSwitchCommand>>> {
        self.termination_command.clone()
    }

    pub fn is_over_check(&self) -> impl Fn() -> bool {
        let termination_command = self.termination_command.clone();
        move || -> bool { termination_command.read().is_some() }
    }

    pub fn can_proceed_check(&self) -> impl Fn() -> bool {
        let header_slices = self.header_slices.clone();
        let fork_header_slices = self.fork_header_slices.clone();
        move || -> bool {
            header_slices.contains_status(HeaderSliceStatus::VerifiedInternally)
                || fork_header_slices.contains_status(HeaderSliceStatus::VerifiedInternally)
        }
    }
}

#[async_trait::async_trait]
impl super::stage::Stage for VerifyLinkForkyStage {
    async fn execute(&mut self) -> anyhow::Result<()> {
        Self::execute(self).await
    }
    fn can_proceed_check(&self) -> Box<dyn Fn() -> bool + Send> {
        Box::new(Self::can_proceed_check(self))
    }
}
