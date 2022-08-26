use super::*;
use crate::{
    kv::{mdbx::*, MdbxWithDirHandle},
    models::*,
    rpc::{eth::EthApiServerImpl, net::NetApiServerImpl, web3::Web3ApiServerImpl},
    TaskGuard,
};
use async_trait::async_trait;
use ethereum_jsonrpc::*;
use jsonrpsee::{
    core::{
        middleware::{Headers, HttpMiddleware, MethodKind},
        server::rpc_module::Methods,
        RpcResult,
    },
    http_server::HttpServerBuilder,
    types::{error::CallError, ErrorObject, Params},
};
use serde::{Deserialize, Serialize};
use std::{future::pending, net::SocketAddr};
use tracing::*;

#[derive(Debug)]
pub struct EngineApiServerImpl {
    chain_tip_sender: watch::Sender<ExternalForkChoice>,
    terminal_total_difficulty: Option<U256>,
    terminal_block_hash: Option<H256>,
    terminal_block_number: Option<BlockNumber>,
}

#[async_trait]
impl EngineApiServer for EngineApiServerImpl {
    async fn new_payload(&self, payload: ExecutionPayload) -> RpcResult<PayloadStatus> {
        let _ = payload;
        Ok(PayloadStatus {
            status: PayloadStatusEnum::Syncing,
            latest_valid_hash: None,
        })
    }
    async fn fork_choice_updated(
        &self,
        fork_choice_state: ForkchoiceState,
        payload_attributes: Option<PayloadAttributes>,
    ) -> RpcResult<ForkchoiceUpdatedResponse> {
        let _ = payload_attributes;
        debug!("Received fork choice information: {fork_choice_state:?}");
        let _ = self.chain_tip_sender.send(ExternalForkChoice {
            head_block: fork_choice_state.head_block_hash,
            finalized_block: fork_choice_state.finalized_block_hash,
        });
        Ok(ForkchoiceUpdatedResponse {
            payload_status: PayloadStatus {
                status: PayloadStatusEnum::Syncing,
                latest_valid_hash: None,
            },
            payload_id: None,
        })
    }
    async fn get_payload(&self, payload_id: H64) -> RpcResult<ExecutionPayload> {
        let _ = payload_id;
        Err(CallError::Custom(ErrorObject::owned(
            -38001,
            String::from("Unknown payload"),
            Option::<String>::None,
        ))
        .into())
    }

    async fn exchange_transition_configuration(
        &self,
        transition_configuration: TransitionConfiguration,
    ) -> RpcResult<TransitionConfiguration> {
        let our_transition_configuration = TransitionConfiguration {
            terminal_total_difficulty: self.terminal_total_difficulty.unwrap_or_default(),
            terminal_block_hash: self.terminal_block_hash.unwrap_or_default(),
            terminal_block_number: self.terminal_block_number.unwrap_or_default().0.into(),
        };

        if transition_configuration != our_transition_configuration {
            error!("Transition configuration mismatch! CL: {transition_configuration:?}, EL: {our_transition_configuration:?}");
        }

        Ok(our_transition_configuration)
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum BeneficiaryFunction {
    #[default]
    Simple,
    Clique,
}

pub type BeneficiarySchedule = BlockSchedule<BeneficiaryFunction>;

#[derive(Debug)]
pub struct BeaconConsensus {
    base: ConsensusEngineBase,
    block_reward: BlockRewardSchedule,
    beneficiary_schedule: BeneficiarySchedule,
    since: Option<BlockNumber>,
    receiver: watch::Receiver<ExternalForkChoice>,
    server_task: Option<TaskGuard<!>>,
}

impl BeaconConsensus {
    pub fn new(
        db: Option<Arc<MdbxWithDirHandle<WriteMap>>>,
        engine_addr: SocketAddr,
        chain_id: ChainId,
        network_id: NetworkId,
        eip1559_block: Option<BlockNumber>,
        block_reward: BlockRewardSchedule,
        beneficiary_schedule: BeneficiarySchedule,
        terminal_total_difficulty: Option<U256>,
        terminal_block_hash: Option<H256>,
        terminal_block_number: Option<BlockNumber>,
        since: Option<BlockNumber>,
    ) -> Self {
        let (chain_tip_sender, receiver) = tokio::sync::watch::channel(ExternalForkChoice {
            head_block: H256::zero(),
            finalized_block: H256::zero(),
        });
        Self {
            base: ConsensusEngineBase::new(chain_id, eip1559_block, Some(32)),
            block_reward,
            beneficiary_schedule,
            since,
            receiver,
            server_task: db.map(move |db| {
                TaskGuard(tokio::spawn(async move {
                    #[derive(Clone)]
                    struct M;

                    impl HttpMiddleware for M {
                        type Instant = ();

                        fn on_request(&self, _: SocketAddr, _: &Headers) -> Self::Instant {}
                        fn on_call(&self, _: &str, _: Params, _: MethodKind) {}
                        fn on_result(&self, name: &str, _: bool, _: Self::Instant) {
                            trace!("Called to {name}");
                        }
                        fn on_response(&self, _: &str, _: Self::Instant) {}
                    }

                    let server = HttpServerBuilder::default()
                        .set_middleware(M)
                        .build(engine_addr)
                        .await
                        .unwrap();

                    let mut api = Methods::new();
                    api.merge(
                        EngineApiServerImpl {
                            chain_tip_sender,
                            terminal_total_difficulty,
                            terminal_block_hash,
                            terminal_block_number,
                        }
                        .into_rpc(),
                    )
                    .unwrap();
                    api.merge(
                        EthApiServerImpl {
                            db,
                            call_gas_limit: 0,
                        }
                        .into_rpc(),
                    )
                    .unwrap();
                    api.merge(NetApiServerImpl { network_id }.into_rpc())
                        .unwrap();
                    api.merge(Web3ApiServerImpl.into_rpc()).unwrap();

                    let server_handle = server.start(api).unwrap();

                    let _server_handle = server_handle;

                    pending().await
                }))
            }),
        }
    }
}

impl Consensus for BeaconConsensus {
    fn name(&self) -> &str {
        "Beacon"
    }

    fn fork_choice_mode(&self) -> ForkChoiceMode {
        ForkChoiceMode::External(self.receiver.clone())
    }

    fn pre_validate_block(
        &self,
        block: &crate::models::Block,
        _: &dyn crate::BlockReader,
    ) -> Result<(), super::DuoError> {
        self.base.pre_validate_block(block)
    }

    fn validate_block_header(
        &self,
        header: &crate::models::BlockHeader,
        parent: &crate::models::BlockHeader,
        with_future_timestamp_check: bool,
    ) -> Result<(), super::DuoError> {
        self.base
            .validate_block_header(header, parent, with_future_timestamp_check)?;

        if self
            .since
            .map(|since| header.number >= since)
            .unwrap_or(true)
        {
            if header.ommers_hash != EMPTY_LIST_HASH {
                return Err(ValidationError::TooManyOmmers.into());
            }

            if header.difficulty != U256::ZERO {
                return Err(ValidationError::WrongDifficulty.into());
            }

            if header.nonce != H64::zero() {
                return Err(ValidationError::WrongHeaderNonce {
                    expected: H64::zero(),
                    got: header.nonce,
                }
                .into());
            }
        }

        Ok(())
    }

    fn get_beneficiary(&self, header: &BlockHeader) -> Address {
        match self.beneficiary_schedule.for_block(header.number) {
            BeneficiaryFunction::Simple => header.beneficiary,
            BeneficiaryFunction::Clique => {
                crate::consensus::clique::recover_signer(header).unwrap()
            }
        }
    }

    fn finalize(
        &self,
        header: &crate::models::BlockHeader,
        ommers: &[crate::models::BlockHeader],
        _transactions: Option<&Vec<MessageWithSender>>,
        _state: &dyn StateReader,
    ) -> anyhow::Result<Vec<super::FinalizationChange>> {
        let block_number = header.number;
        let block_reward = self.block_reward.for_block(block_number);

        Ok(if block_reward > 0 {
            let mut changes = Vec::with_capacity(1 + ommers.len());

            let mut miner_reward = block_reward;
            for ommer in ommers {
                let ommer_reward =
                    (U256::from(8 + ommer.number.0 - block_number.0) * block_reward) >> 3;
                changes.push(FinalizationChange::Reward {
                    address: ommer.beneficiary,
                    amount: ommer_reward,
                    ommer: true,
                });
                miner_reward += block_reward / 32;
            }

            changes.push(FinalizationChange::Reward {
                address: header.beneficiary,
                amount: miner_reward,
                ommer: false,
            });

            changes
        } else {
            vec![]
        })
    }
}
