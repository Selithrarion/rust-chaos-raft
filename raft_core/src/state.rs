use crate::log::LogEntry;
use crate::storage::SnapshotMetadata;
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub type Term = u64;
pub type NodeId = u64;

#[derive(Debug, Clone, PartialEq, Eq, Copy, Deserialize, Serialize)]
pub enum NodeRole {
    Follower,
    Candidate,
    Leader,
}

pub struct RaftNodeState {
    pub id: NodeId,

    // disk state
    pub current_term: Term,
    pub voted_for: Option<NodeId>,
    pub log: Vec<LogEntry>,

    // ram state
    pub commit_index: u64,
    pub last_applied: u64,
    pub snapshot_metadata: SnapshotMetadata,

    // role specific
    pub role: NodeRole,
    pub leader_id: Option<NodeId>,

    // election timer state
    pub election_elapsed_ticks: u64,
    pub election_timeout_ticks: u64,

    // db
    pub state_machine: HashMap<String, Vec<u8>>,
}

impl RaftNodeState {
    pub fn new(
        id: NodeId,
        current_term: Term,
        voted_for: Option<NodeId>,
        log: Vec<LogEntry>,
        snapshot_metadata: SnapshotMetadata,
        state_machine: HashMap<String, Vec<u8>>,
    ) -> Self {
        Self {
            id,
            current_term,
            voted_for,
            log,
            commit_index: 0,
            last_applied: snapshot_metadata.last_included_index,
            snapshot_metadata,
            role: NodeRole::Follower,
            leader_id: None,
            election_elapsed_ticks: 0,
            election_timeout_ticks: 0,
            state_machine,
        }
    }

    pub fn reset_election_timeout(&mut self) {
        self.election_elapsed_ticks = 0;
        self.election_timeout_ticks = rand::rng().random_range(10..20);
    }
}
