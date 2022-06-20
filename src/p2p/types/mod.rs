mod block;
mod header;
mod message;
mod penalty;
mod rlp;
mod status;

pub use self::{block::*, header::*, message::*, penalty::*, rlp::*, status::*};

use super::node::SentryId;
use crate::sentry::devp2p::PeerId;

#[derive(Clone, Debug, PartialEq)]
pub enum PeerFilter {
    All,
    Random(u64),
    Peer(PeerId, SentryId),
    MinBlock(u64),
}
