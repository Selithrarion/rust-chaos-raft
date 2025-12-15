use crate::log::LogEntry;
use crate::state::{NodeId, Term};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RaftMessage {
    RequestVote {
        term: Term,
        candidate_id: NodeId,
        last_log_index: u64,
        last_log_term: Term,
    },
    RequestVoteResponse {
        term: Term,
        vote_granted: bool,
    },
    AppendEntries {
        term: Term,
        leader_id: NodeId,
        prev_log_index: u64,
        prev_log_term: Term,
        entries: Vec<LogEntry>, // empty for heartbeat
        leader_commit: u64,
    },
    AppendEntriesResponse {
        term: Term,
        success: bool,
        match_index: u64,
    },
    InstallSnapshot {
        term: Term,
        leader_id: NodeId,
        last_included_index: u64,
        last_included_term: Term,
        data: Vec<u8>,
    },
    InstallSnapshotResponse {
        term: Term,
    },
}
