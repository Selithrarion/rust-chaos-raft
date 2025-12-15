use crate::message::RaftMessage;
use crate::state::{NodeId, NodeRole, Term};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ClientMessage {
    Request { command: Vec<u8> },
    GetStatus,
    NoOp,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum ClientQueryResponse {
    Status(NodeStatusInfo),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct NodeStatusInfo {
    pub role: NodeRole,
    pub term: Term,
    pub leader_id: Option<NodeId>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ClientResponse {
    Success,
    NotLeader(Option<NodeId>),
}
#[derive(Serialize, Deserialize, Debug)]
pub struct WireMessage {
    pub sender_id: NodeId,
    pub message: RaftMessage,
}
