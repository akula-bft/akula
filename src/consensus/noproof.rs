use super::*;

#[derive(Debug)]
pub struct NoProof {
    base: Ethash,
}

impl NoProof {
    pub fn new(chain_config: ChainConfig) -> Self {
        Self {
            base: Ethash::new(chain_config),
        }
    }
}

#[async_trait]
impl Consensus for NoProof {
    async fn pre_validate_block(&self, block: &Block, state: &mut dyn State) -> anyhow::Result<()> {
        self.base.pre_validate_block(block, state).await
    }

    async fn validate_block_header(
        &self,
        header: &BlockHeader,
        state: &mut dyn State,
        with_future_timestamp_check: bool,
    ) -> anyhow::Result<()> {
        self.base
            .validate_block_header(header, state, with_future_timestamp_check)
            .await
    }

    async fn validate_seal(&self, _: &BlockHeader) -> anyhow::Result<()> {
        Ok(())
    }

    async fn finalize(
        &self,
        block: &PartialHeader,
        ommers: &[BlockHeader],
        revision: Revision,
    ) -> anyhow::Result<Vec<FinalizationChange>> {
        self.base.finalize(block, ommers, revision).await
    }

    /// See [YP] Section 11.3 "Reward Application".
    async fn get_beneficiary(&self, header: &BlockHeader) -> anyhow::Result<Address> {
        self.base.get_beneficiary(header).await
    }
}
