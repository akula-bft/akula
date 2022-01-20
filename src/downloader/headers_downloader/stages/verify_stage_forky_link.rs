use super::{
    fork_mode_stage::ForkModeStage,
    headers::header_slices::{HeaderSliceStatus, HeaderSlices},
    verification::header_slice_verifier::HeaderSliceVerifier,
    verify_stage_linear_link::VerifyStageLinearLink,
};
use crate::{models::BlockNumber, sentry::chain_config::ChainConfig};
use std::sync::Arc;
use tracing::*;

/// Verifies the sequence rules to grow the slices chain and sets Verified status.
pub struct VerifyStageForkyLink {
    header_slices: Arc<HeaderSlices>,
    chain_config: ChainConfig,
    verifier: Arc<Box<dyn HeaderSliceVerifier>>,
    start_block_num: BlockNumber,
    start_block_hash: ethereum_types::H256,
    mode: Mode,
}

enum Mode {
    Linear(Box<VerifyStageLinearLink>),
    Fork(Box<ForkModeStage>),
}

impl VerifyStageForkyLink {
    pub fn new(
        header_slices: Arc<HeaderSlices>,
        chain_config: ChainConfig,
        verifier: Arc<Box<dyn HeaderSliceVerifier>>,
        start_block_num: BlockNumber,
        start_block_hash: ethereum_types::H256,
    ) -> Self {
        let linear_mode_stage = VerifyStageLinearLink::new(
            header_slices.clone(),
            chain_config.clone(),
            verifier.clone(),
            start_block_num,
            start_block_hash,
            HeaderSliceStatus::Fork,
        );

        Self {
            header_slices,
            chain_config,
            verifier,
            start_block_num,
            start_block_hash,
            mode: Mode::Linear(Box::new(linear_mode_stage)),
        }
    }

    pub async fn execute(&mut self) -> anyhow::Result<()> {
        // execute sub-stage
        let sub_stage: &mut dyn super::stage::Stage = match self.mode {
            Mode::Linear(ref mut stage) => {
                debug!("VerifyStageForkyLink: Mode::Linear");
                stage.as_mut()
            }
            Mode::Fork(ref mut stage) => {
                debug!("VerifyStageForkyLink: Mode::Fork");
                stage.as_mut()
            }
        };
        sub_stage.execute().await?;

        // check mode switch conditions
        match self.mode {
            Mode::Linear(_) => {
                let fork_slice_count = self
                    .header_slices
                    .count_slices_in_status(HeaderSliceStatus::Fork);
                if fork_slice_count > 0 {
                    debug!("VerifyStageForkyLink: switching to Mode::Fork");
                    self.switch_to_fork_mode();
                }
            }
            Mode::Fork(ref stage) => {
                if stage.is_done() {
                    debug!("VerifyStageForkyLink: switching to Mode::Linear");
                    self.switch_to_linear_mode();
                }
            }
        }

        Ok(())
    }

    fn make_linear_mode_stage(&self) -> VerifyStageLinearLink {
        VerifyStageLinearLink::new(
            self.header_slices.clone(),
            self.chain_config.clone(),
            self.verifier.clone(),
            self.start_block_num,
            self.start_block_hash,
            HeaderSliceStatus::Fork,
        )
    }

    fn switch_to_linear_mode(&mut self) {
        self.mode = Mode::Linear(Box::new(self.make_linear_mode_stage()));
    }

    fn make_fork_mode_stage(&self) -> ForkModeStage {
        ForkModeStage::new(
            self.header_slices.clone(),
            self.chain_config.clone(),
            self.verifier.clone(),
        )
    }

    fn switch_to_fork_mode(&mut self) {
        let mut fork_mode_stage = self.make_fork_mode_stage();
        fork_mode_stage.setup();
        self.mode = Mode::Fork(Box::new(fork_mode_stage));
    }

    pub fn can_proceed_check(&self) -> impl Fn() -> bool {
        let header_slices = self.header_slices.clone();
        move || -> bool { header_slices.contains_status(HeaderSliceStatus::VerifiedInternally) }
    }
}

#[async_trait::async_trait]
impl super::stage::Stage for VerifyStageForkyLink {
    async fn execute(&mut self) -> anyhow::Result<()> {
        Self::execute(self).await
    }
    fn can_proceed_check(&self) -> Box<dyn Fn() -> bool + Send> {
        Box::new(Self::can_proceed_check(self))
    }
}
