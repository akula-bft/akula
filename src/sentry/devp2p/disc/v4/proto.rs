use super::message::*;
use enum_primitive_derive::Primitive;
use tokio::sync::oneshot::Sender as OneshotSender;

#[derive(Primitive)]
pub enum MessageId {
    Ping = 1,
    Pong = 2,
    FindNode = 3,
    Neighbours = 4,
}

#[derive(Debug)]
pub enum EgressMessage {
    Ping(PingMessage, Option<OneshotSender<()>>),
    Pong(PongMessage),
    FindNode(FindNodeMessage),
    Neighbours(NeighboursMessage),
}
