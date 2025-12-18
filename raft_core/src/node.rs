use crate::log::LogEntry;
use crate::message::RaftMessage;
use crate::network::Network;
use crate::protocol::NodeStatusInfo;
use crate::state::{NodeId, NodeRole, RaftNodeState, Term};
use crate::storage::SnapshotMetadata;
use crate::storage::Storage;
use anyhow::anyhow;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::{Duration, MissedTickBehavior, interval};

const SNAPSHOT_THRESHOLD: usize = 10;

#[derive(Debug)]
pub enum Event {
    Rpc {
        sender_id: NodeId,
        message: RaftMessage,
    },
    Tick,
    ClientCommand {
        command: Vec<u8>,
        responder: oneshot::Sender<anyhow::Result<()>>,
    },
}

pub struct RaftNode {
    state: RaftNodeState,
    event_rx: mpsc::Receiver<Event>,
    peers: Vec<NodeId>,
    votes_received: u64,
    storage: Arc<Storage>,
    status_tx: watch::Sender<NodeStatusInfo>,
    network: Arc<dyn Network>,

    // leader state
    next_indices: HashMap<NodeId, u64>,
    match_indices: HashMap<NodeId, u64>,
    pending_responses: HashMap<u64, oneshot::Sender<anyhow::Result<()>>>,
}

impl RaftNode {
    pub fn new(
        id: NodeId,
        peers: Vec<NodeId>,
        network: Arc<dyn Network>,
        storage: Arc<Storage>,
        status_tx: watch::Sender<NodeStatusInfo>,
        event_rx: mpsc::Receiver<Event>,
    ) -> Self {
        let (term, voted_for) = storage.load_metadata().unwrap_or((0, None));
        let (log, snapshot_metadata, state_machine) =
            if let Ok(Some((metadata, sm))) = storage.load_snapshot() {
                let log = storage.load_log().unwrap_or_default();
                (log, metadata, sm)
            } else {
                (
                    storage.load_log().unwrap_or_default(),
                    SnapshotMetadata::default(),
                    HashMap::new(),
                )
            };

        let mut state: RaftNodeState =
            RaftNodeState::new(id, term, voted_for, log, snapshot_metadata, state_machine);
        state.reset_election_timeout();

        let node = Self {
            state,
            event_rx,
            peers,
            storage,
            status_tx,
            votes_received: 0,
            network,
            next_indices: HashMap::new(),
            match_indices: HashMap::new(),
            pending_responses: HashMap::new(),
        };

        node
    }

    pub async fn run(mut self) {
        let mut ticker = interval(Duration::from_millis(150));
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

        println!(
            "[Node {}] Starting as a {:?} in term {}",
            self.state.id, self.state.role, self.state.current_term
        );

        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    self.handle_event(Event::Tick).await;
                }
                Some(event) = self.event_rx.recv() => {
                    self.handle_event(event).await;
                }
            }
        }
    }

    async fn handle_event(&mut self, event: Event) {
        let should_log = match &event {
            Event::Tick => false,
            Event::Rpc { message, .. } => match message {
                RaftMessage::AppendEntries { entries, .. } if entries.is_empty() => false,
                RaftMessage::AppendEntriesResponse { .. } => false,
                _ => true,
            },
            _ => true,
        };
        if should_log {
            println!("[Node {}] Handling event: {:?}", self.state.id, &event);
        }

        match event {
            Event::Tick => self.handle_tick().await,
            Event::Rpc { sender_id, message } => {
                self.step_down_if_term_is_higher(&message).await;
                self.handle_rpc(sender_id, message).await;
            }
            Event::ClientCommand { command, responder } => {
                self.handle_client_command(command, responder).await;
            }
        }

        self.apply_committed_entries();

        let should_snapshot = self.state.last_applied
            > self.state.snapshot_metadata.last_included_index + SNAPSHOT_THRESHOLD as u64;
        if self.state.role == NodeRole::Leader && should_snapshot {
            self.create_snapshot().await;
        }
    }

    async fn handle_tick(&mut self) {
        match self.state.role {
            NodeRole::Follower | NodeRole::Candidate => {
                self.state.election_elapsed_ticks += 1;
                if self.state.election_elapsed_ticks >= self.state.election_timeout_ticks {
                    println!(
                        "[Node {}] Election timeout! Becoming a candidate",
                        self.state.id
                    );
                    self.become_candidate().await;
                }
            }
            NodeRole::Leader => {
                self.replicate_log_entries().await;
            }
        }

        let status_info = NodeStatusInfo {
            role: self.state.role,
            term: self.state.current_term,
            commit_index: self.state.commit_index,
            leader_id: self.state.leader_id,
        };
        if self.status_tx.send(status_info).is_err() {
            // no receivers
        }
    }

    async fn become_candidate(&mut self) {
        self.state.current_term += 1;
        self.state.role = NodeRole::Candidate;
        self.state.voted_for = Some(self.state.id);
        self.state.leader_id = None;
        self.storage
            .save_metadata(self.state.current_term, self.state.voted_for)
            .expect("Failed to save metadata");

        self.votes_received = 1;
        self.state.reset_election_timeout();

        let request = RaftMessage::RequestVote {
            term: self.state.current_term,
            candidate_id: self.state.id,
            last_log_index: self
                .state
                .log
                .last()
                .map_or(self.state.snapshot_metadata.last_included_index, |e| {
                    e.index
                }),
            last_log_term: self
                .state
                .log
                .last()
                .map_or(self.state.snapshot_metadata.last_included_term, |e| e.term),
        };

        for peer_id in &self.peers {
            println!(
                "[Node {}] Sending RequestVote to {}",
                self.state.id, peer_id
            );
            self.network
                .send(self.state.id, *peer_id, request.clone())
                .await
                .unwrap_or_else(|e| {
                    eprintln!(
                        "[Node {}] Failed to send RequestVote to {}: {}",
                        self.state.id, peer_id, e
                    );
                });
        }
    }

    async fn handle_rpc(&mut self, sender_id: NodeId, message: RaftMessage) {
        match message {
            RaftMessage::RequestVote {
                term,
                candidate_id,
                last_log_index,
                last_log_term,
            } => {
                self.handle_request_vote(
                    sender_id,
                    term,
                    candidate_id,
                    last_log_index,
                    last_log_term,
                )
                .await;
            }
            RaftMessage::RequestVoteResponse { term, vote_granted } => {
                self.handle_request_vote_response(sender_id, term, vote_granted)
                    .await;
            }
            RaftMessage::AppendEntries {
                term,
                leader_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            } => {
                self.handle_append_entries(
                    term,
                    leader_id,
                    prev_log_index,
                    prev_log_term,
                    entries,
                    leader_commit,
                )
                .await;
            }
            RaftMessage::AppendEntriesResponse {
                term,
                success,
                match_index,
            } => {
                self.handle_append_entries_response(sender_id, term, success, match_index)
                    .await;
            }
            RaftMessage::InstallSnapshot {
                term,
                leader_id,
                last_included_index,
                last_included_term,
                data,
            } => {
                self.handle_install_snapshot(
                    term,
                    leader_id,
                    last_included_index,
                    last_included_term,
                    data,
                )
                .await;
            }
            RaftMessage::InstallSnapshotResponse { term } => {
                self.handle_install_snapshot_response(sender_id, term);
            }
        }
    }

    async fn handle_client_command(
        &mut self,
        command: Vec<u8>,
        responder: oneshot::Sender<anyhow::Result<()>>,
    ) {
        if self.state.role != NodeRole::Leader {
            println!(
                "[Node {}] I am not a leader, ignoring client command",
                self.state.id
            );
            let _ = responder.send(Err(anyhow!(
                "Not a leader. Current leader is {:?}",
                self.state.leader_id
            )));
            return;
        }

        let last_known_index = self
            .state
            .log
            .last()
            .map_or(self.state.snapshot_metadata.last_included_index, |e| {
                e.index
            });
        let new_log_index = last_known_index + 1;
        self.pending_responses.insert(new_log_index, responder);

        let entry = LogEntry {
            term: self.state.current_term,
            index: new_log_index,
            command: command.clone(),
        };

        let storage = self.storage.clone();
        let entry_clone = entry.clone();
        tokio::task::spawn_blocking(move || storage.append_log_entry(&entry_clone))
            .await
            .expect("Task panicked")
            .expect("Failed to append log entry");
        println!(
            "[Node {}] Leader received command, appending to log: {:?}",
            self.state.id, entry
        );
        self.state.log.push(entry);
    }

    async fn handle_request_vote(
        &mut self,
        sender_id: NodeId,
        term: Term,
        candidate_id: NodeId,
        candidate_last_log_index: u64,
        candidate_last_log_term: Term,
    ) {
        let term_is_ok = term >= self.state.current_term;

        let can_vote_in_term =
            self.state.voted_for.is_none() || self.state.voted_for == Some(candidate_id);

        let our_last_log_index = self
            .state
            .log
            .last()
            .map_or(self.state.snapshot_metadata.last_included_index, |e| {
                e.index
            });
        let our_last_log_term = self
            .state
            .log
            .last()
            .map_or(self.state.snapshot_metadata.last_included_term, |e| e.term);

        let log_is_up_to_date = candidate_last_log_term > our_last_log_term
            || (candidate_last_log_term == our_last_log_term
                && candidate_last_log_index >= our_last_log_index);

        let vote_granted = term_is_ok && can_vote_in_term && log_is_up_to_date;

        if vote_granted {
            println!(
                "[Node {}] Granting vote for {} in term {}",
                self.state.id, candidate_id, term
            );
            self.state.voted_for = Some(candidate_id);
            self.state.role = NodeRole::Follower;
            self.storage
                .save_metadata(self.state.current_term, self.state.voted_for)
                .expect("Failed to save metadata");
            self.state.reset_election_timeout();
        } else {
            println!(
                "[Node {}] Denying vote for {} in term {}",
                self.state.id, candidate_id, term
            );
        }

        let response = RaftMessage::RequestVoteResponse {
            term: self.state.current_term,
            vote_granted,
        };

        println!(
            "[Node {}] Responding to RequestVote from {}: Granted={}",
            self.state.id, candidate_id, vote_granted
        );
        self.network
            .send(self.state.id, sender_id, response)
            .await
            .unwrap_or_else(|e| {
                eprintln!(
                    "[Node {}] Failed to send RequestVoteResponse to {}: {}",
                    self.state.id, candidate_id, e
                );
            });
    }

    async fn handle_request_vote_response(
        &mut self,
        _sender_id: NodeId,
        term: Term,
        vote_granted: bool,
    ) {
        if self.state.role != NodeRole::Candidate || term != self.state.current_term {
            return;
        }

        if vote_granted {
            self.votes_received += 1;
            println!(
                "[Node {}] Vote received. Total votes: {}",
                self.state.id, self.votes_received
            );

            let required_votes = (self.peers.len() as u64 + 1) / 2 + 1;
            if self.votes_received >= required_votes {
                println!(
                    "[Node {}] Majority of votes received! Becoming leader",
                    self.state.id
                );
                self.become_leader().await;
            }
        }
    }

    async fn become_leader(&mut self) {
        self.state.role = NodeRole::Leader;
        self.state.leader_id = Some(self.state.id);
        println!(
            "\n====================\n[Node {}] I AM THE LEADER FOR TERM {}!\n====================\n",
            self.state.id, self.state.current_term
        );

        let last_known_index = self
            .state
            .log
            .last()
            .map_or(self.state.snapshot_metadata.last_included_index, |e| {
                e.index
            });
        let next_log_index = last_known_index + 1;
        for peer_id in &self.peers {
            self.next_indices.insert(*peer_id, next_log_index);
            self.match_indices.insert(*peer_id, 0);
        }

        self.replicate_log_entries().await;
    }

    async fn handle_append_entries_response(
        &mut self,
        sender_id: NodeId,
        term: Term,
        success: bool,
        match_index: u64,
    ) {
        if self.state.role != NodeRole::Leader || term != self.state.current_term {
            return;
        }

        if success {
            self.match_indices.insert(sender_id, match_index);
            self.next_indices.insert(sender_id, match_index + 1);
        } else {
            let current_next_index = self.next_indices.get(&sender_id).cloned().unwrap_or(2);
            let new_next_index = (current_next_index / 2).max(1);
            self.next_indices.insert(sender_id, new_next_index);
            println!(
                "[Node {}] Follower {} rejected AppendEntries. Retrying with next_index={}",
                self.state.id, sender_id, new_next_index
            );

            self.replicate_log_to_peer(sender_id).await;
        }

        self.advance_commit_index().await;
    }

    async fn replicate_log_to_peer(&mut self, peer_id: NodeId) {
        let next_index = *self.next_indices.get(&peer_id).unwrap_or(&1);

        let should_send_snapshot = next_index <= self.state.snapshot_metadata.last_included_index;
        if should_send_snapshot {
            println!(
                "[Node {}] Follower {} is too far behind. Sending snapshot.",
                self.state.id, peer_id
            );
            self.next_indices.insert(
                peer_id,
                self.state.snapshot_metadata.last_included_index + 1,
            );
            self.send_install_snapshot(peer_id).await;
            return;
        }

        let prev_log_index = next_index - 1;
        let prev_log_term = if prev_log_index == self.state.snapshot_metadata.last_included_index {
            self.state.snapshot_metadata.last_included_term
        } else if prev_log_index > 0 {
            self.state
                .log
                .iter()
                .find(|e| e.index == prev_log_index)
                .map_or(0, |e| e.term)
        } else {
            0
        };
        let entries: Vec<LogEntry> = self
            .state
            .log
            .iter()
            .filter(|e| e.index >= next_index)
            .cloned()
            .collect();

        let request = RaftMessage::AppendEntries {
            term: self.state.current_term,
            leader_id: self.state.id,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit: self.state.commit_index,
        };

        self.network
            .send(self.state.id, peer_id, request)
            .await
            .unwrap_or_else(|_e| {});
    }

    async fn advance_commit_index(&mut self) {
        let mut sorted_matches: Vec<u64> = self.match_indices.values().cloned().collect();
        let leader_last_index = self
            .state
            .log
            .last()
            .map_or(self.state.snapshot_metadata.last_included_index, |e| {
                e.index
            });
        sorted_matches.push(leader_last_index);
        sorted_matches.sort_unstable();
        sorted_matches.reverse();

        let required_votes = (self.peers.len() as u64 + 1) / 2 + 1;
        let majority_match_index = sorted_matches[(required_votes - 1) as usize];

        if majority_match_index > self.state.commit_index {
            if let Some(entry) = self
                .state
                .log
                .iter()
                .find(|e| e.index == majority_match_index)
            {
                if entry.term == self.state.current_term {
                    println!(
                        "[Node {}] Advancing commit_index to {}",
                        self.state.id, majority_match_index
                    );
                    self.state.commit_index = majority_match_index;

                    while let Some(entry) = self.pending_responses.keys().next().cloned() {
                        if entry <= self.state.commit_index {
                            if let Some(responder) = self.pending_responses.remove(&entry) {
                                let _ = responder.send(Ok(()));
                            }
                        } else {
                            break;
                        }
                    }
                }
            }
        }
    }

    async fn handle_append_entries(
        &mut self,
        term: Term,
        leader_id: NodeId,
        prev_log_index: u64,
        prev_log_term: Term,
        entries: Vec<LogEntry>,
        leader_commit: u64,
    ) {
        if term < self.state.current_term {
            let response = RaftMessage::AppendEntriesResponse {
                term: self.state.current_term,
                success: false,
                match_index: 0,
            };
            self.network
                .send(self.state.id, leader_id, response)
                .await
                .unwrap_or_else(|e| {
                    eprintln!(
                        "[Node {}] Failed to send negative AppendEntriesResponse to {}: {}",
                        self.state.id, leader_id, e
                    );
                });
            return;
        }

        if !entries.is_empty() {
            println!(
                "[Node {}] AppendEntries received from leader {}. Resetting election timer",
                self.state.id, leader_id
            );
        }
        self.state.role = NodeRole::Follower;
        self.state.leader_id = Some(leader_id);
        self.state.reset_election_timeout();

        if prev_log_index > 0 {
            let is_log_consistent = if prev_log_index
                == self.state.snapshot_metadata.last_included_index
            {
                prev_log_term == self.state.snapshot_metadata.last_included_term
            } else {
                let find_result = self.state.log.iter().find(|e| e.index == prev_log_index);
                if find_result.is_none() {
                    println!(
                        "[DEBUG][FOLLOWER {}] Log content on failed find for prev_log_index={}: {:?}",
                        self.state.id, prev_log_index, self.state.log
                    );
                }
                find_result.map_or(false, |e| e.term == prev_log_term)
            };

            if !is_log_consistent {
                println!(
                    "[Node {}] Log conflict with leader {}. Replying false",
                    self.state.id, leader_id
                );
                let response = RaftMessage::AppendEntriesResponse {
                    term: self.state.current_term,
                    success: false,
                    match_index: self.state.log.last().map_or(0, |e| e.index),
                };
                self.network
                    .send(self.state.id, leader_id, response)
                    .await
                    .unwrap_or_else(|e| {
                        eprintln!(
                            "[Node {}] Failed to send negative AppendEntriesResponse to {}: {}",
                            self.state.id, leader_id, e
                        );
                    });
                return;
            }
        }

        let mut new_entries_start_index = 0;
        for (i, entry) in entries.iter().enumerate() {
            if let Some(log_entry) = self.state.log.iter().find(|e| e.index == entry.index) {
                if log_entry.term != entry.term {
                    println!(
                        "[Node {}] Conflict at index {}. Truncating log.",
                        self.state.id, entry.index
                    );
                    let storage = self.storage.clone();
                    let index_to_truncate = entry.index;
                    self.state.log.retain(|e| e.index < entry.index);
                    tokio::task::spawn_blocking(move || {
                        storage.discard_log_after(index_to_truncate)
                    })
                    .await
                    .expect("Task panicked")
                    .expect("Failed to truncate log file");
                    break;
                }
            } else {
                new_entries_start_index = i;
                break;
            }
            new_entries_start_index = i + 1;
        }

        for entry in entries.iter().skip(new_entries_start_index) {
            println!("[Node {}] Appending new entry: {:?}", self.state.id, entry);
            let storage = self.storage.clone();
            let entry_clone = entry.clone();
            tokio::task::spawn_blocking(move || storage.append_log_entry(&entry_clone))
                .await
                .expect("Task panicked")
                .expect("Failed to append log entry");
            self.state.log.push(entry.clone());
        }

        if leader_commit > self.state.commit_index {
            let last_log_index = self
                .state
                .log
                .last()
                .map_or(self.state.snapshot_metadata.last_included_index, |e| {
                    e.index
                });
            self.state.commit_index = std::cmp::min(leader_commit, last_log_index);
        }

        let response = RaftMessage::AppendEntriesResponse {
            term: self.state.current_term,
            success: true,
            match_index: entries.last().map_or(prev_log_index, |e| e.index),
        };
        self.network
            .send(self.state.id, leader_id, response)
            .await
            .unwrap_or_else(|e| {
                eprintln!(
                    "[Node {}] Failed to send positive AppendEntriesResponse to {}: {}",
                    self.state.id, leader_id, e
                );
            });
    }

    async fn replicate_log_entries(&mut self) {
        for peer_id in self.peers.clone() {
            self.replicate_log_to_peer(peer_id).await;
        }
    }

    async fn step_down_if_term_is_higher(&mut self, message: &RaftMessage) {
        let message_term = match message {
            RaftMessage::RequestVote { term, .. } => *term,
            RaftMessage::RequestVoteResponse { term, .. } => *term,
            RaftMessage::AppendEntries { term, .. } => *term,
            RaftMessage::AppendEntriesResponse { term, .. } => *term,
            RaftMessage::InstallSnapshot { term, .. } => *term,
            RaftMessage::InstallSnapshotResponse { term, .. } => *term,
        };

        if message_term > self.state.current_term {
            println!(
                "[Node {}] Received message with higher term {}. Stepping down",
                self.state.id, message_term
            );
            self.state.current_term = message_term;
            self.state.role = NodeRole::Follower;
            self.state.voted_for = None;
            self.state.leader_id = None;
            self.storage
                .save_metadata(self.state.current_term, self.state.voted_for)
                .expect("Failed to save metadata");
        }
    }

    fn apply_committed_entries(&mut self) {
        while self.state.commit_index > self.state.last_applied {
            let apply_index = self.state.last_applied + 1;
            if let Some(entry) = self.state.log.iter().find(|e| e.index == apply_index) {
                println!(
                    "[Node {}] Applying entry {} to state machine: {:?}",
                    self.state.id, apply_index, entry.command
                );

                if entry.command != b"PING" && entry.command != b"NOOP" {
                    if let Ok(command_str) = std::str::from_utf8(&entry.command) {
                        if let Some((op, rest)) = command_str.split_once(' ') {
                            if op == "SET" {
                                if let Some((key, value)) = rest.split_once('=') {
                                    self.state
                                        .state_machine
                                        .insert(key.to_string(), value.to_string().into_bytes());
                                }
                            }
                        }
                    }
                }
                self.state.last_applied = apply_index;
            } else {
                let is_entry_covered_by_snapshot =
                    apply_index <= self.state.snapshot_metadata.last_included_index;
                if is_entry_covered_by_snapshot {
                    self.state.last_applied = apply_index;
                }
                break;
            }
        }
    }

    async fn handle_install_snapshot(
        &mut self,
        term: Term,
        leader_id: NodeId,
        last_included_index: u64,
        last_included_term: Term,
        data: Vec<u8>,
    ) {
        let is_stale_snapshot = term < self.state.current_term;
        if is_stale_snapshot {
            return;
        }

        println!(
            "[Node {}] Received InstallSnapshot from leader {}",
            self.state.id, leader_id
        );

        let snapshot_metadata = SnapshotMetadata {
            last_included_index,
            last_included_term,
        };
        let state_machine: HashMap<String, Vec<u8>> = bincode::deserialize(&data).unwrap();

        let storage = self.storage.clone();
        let metadata_clone = snapshot_metadata;
        let sm_clone = state_machine.clone();
        tokio::task::spawn_blocking(move || storage.save_snapshot(&metadata_clone, &sm_clone))
            .await
            .expect("Task panicked")
            .expect("Failed to save received snapshot");

        self.state.log.clear();
        let storage_2 = self.storage.clone();
        tokio::task::spawn_blocking(move || storage_2.discard_log_after(0))
            .await
            .expect("Task panicked")
            .expect("Failed to discard log after snapshot install");

        self.state.snapshot_metadata = snapshot_metadata;
        self.state.state_machine = state_machine;
        self.state.commit_index = last_included_index;
        self.state.last_applied = last_included_index;

        let response = RaftMessage::InstallSnapshotResponse {
            term: self.state.current_term,
        };
        self.network
            .send(self.state.id, leader_id, response)
            .await
            .unwrap_or_else(|_| {});
    }

    fn handle_install_snapshot_response(&mut self, sender_id: NodeId, term: Term) {
        if self.state.role != NodeRole::Leader || term != self.state.current_term {
            return;
        }
        println!(
            "[Node {}] Received InstallSnapshotResponse from {}",
            self.state.id, sender_id
        );
        self.match_indices
            .insert(sender_id, self.state.snapshot_metadata.last_included_index);
        self.next_indices.insert(
            sender_id,
            self.state.snapshot_metadata.last_included_index + 1,
        );
    }

    async fn create_snapshot(&mut self) {
        let last_applied = self.state.last_applied;
        let last_entry_term = if let Some(last_entry) =
            self.state.log.iter().find(|e| e.index == last_applied)
        {
            last_entry.term
        } else if last_applied == self.state.snapshot_metadata.last_included_index {
            self.state.snapshot_metadata.last_included_term
        } else {
            eprintln!(
                "[Node {}] Cannot find term for last_applied index {}. Snapshot creation skipped.",
                self.state.id, last_applied
            );
            return;
        };

        if last_applied == self.state.snapshot_metadata.last_included_index {
            return;
        };

        let snapshot_metadata = SnapshotMetadata {
            last_included_index: last_applied,
            last_included_term: last_entry_term,
        };

        println!(
            "[Node {}] Creating snapshot at index {}",
            self.state.id, last_applied
        );

        let storage = self.storage.clone();
        let metadata_clone = snapshot_metadata;
        let sm_clone = self.state.state_machine.clone();
        tokio::task::spawn_blocking(move || storage.save_snapshot(&metadata_clone, &sm_clone))
            .await
            .expect("Task panicked")
            .expect("Failed to save snapshot");

        let storage_2 = self.storage.clone();
        let last_applied_clone = last_applied;
        tokio::task::spawn_blocking(move || storage_2.discard_log_before(last_applied_clone + 1))
            .await
            .expect("Task panicked")
            .expect("Failed to compact log on disk");

        self.state.log.retain(|entry| entry.index > last_applied);
        self.state.snapshot_metadata = snapshot_metadata;

        println!("[Node {}] Snapshot created. Log compacted.", self.state.id);
    }

    async fn send_install_snapshot(&self, peer_id: NodeId) {
        let Some((metadata, state_machine)) = self.storage.load_snapshot().unwrap_or(None) else {
            eprintln!(
                "[Node {}] Could not load snapshot to send to {}",
                self.state.id, peer_id
            );
            return;
        };

        let data = bincode::serialize(&state_machine).unwrap();

        let request = RaftMessage::InstallSnapshot {
            term: self.state.current_term,
            leader_id: self.state.id,
            last_included_index: metadata.last_included_index,
            last_included_term: metadata.last_included_term,
            data,
        };

        self.network
            .send(self.state.id, peer_id, request)
            .await
            .unwrap_or_else(|_| {});
    }
}
