use crate::sentry::devp2p::PeerId;
use ethereum_interfaces::sentry as grpc_sentry;

#[derive(Debug, Clone, Default)]
pub enum PenaltyKind {
    #[default]
    BadBlock,
    DuplicateHeader,
    WrongChildBlockHeight,
    WrongChildDifficulty,
    InvalidSeal,
    TooFarFuture,
    TooFarPast,
}

#[derive(Debug, Clone)]
pub struct Penalty {
    pub peer_id: PeerId,
    pub kind: PenaltyKind,
}

impl const From<Penalty> for grpc_sentry::PenalizePeerRequest {
    #[inline(always)]
    fn from(penalty: Penalty) -> Self {
        grpc_sentry::PenalizePeerRequest {
            peer_id: Some(penalty.peer_id.into()),
            penalty: 0,
        }
    }
}
