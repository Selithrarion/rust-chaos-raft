use crate::message::RaftMessage;
use crate::network::Network;
use crate::node::Event;
use crate::state::NodeId;
use async_trait::async_trait;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;

#[derive(Clone)]
pub struct InMemoryNetwork {
    senders: Arc<Mutex<HashMap<NodeId, mpsc::Sender<Event>>>>,
    partitions: Arc<Mutex<HashSet<(NodeId, NodeId)>>>,
}

impl InMemoryNetwork {
    pub fn new() -> Self {
        Self {
            senders: Arc::new(Mutex::new(HashMap::new())),
            partitions: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    pub fn add_node(&self, id: NodeId, sender: mpsc::Sender<Event>) {
        self.senders.lock().unwrap().insert(id, sender);
    }

    pub fn partition(&self, from: NodeId, to: NodeId) {
        println!("[NEMESIS] Cutting connection from {} to {}", from, to);
        self.partitions.lock().unwrap().insert((from, to));
    }

    pub fn heal(&self, from: NodeId, to: NodeId) {
        println!("[NEMESIS] Healing connection from {} to {}", from, to);
        self.partitions.lock().unwrap().remove(&(from, to));
    }

    pub fn heal_all(&self) {
        println!("[NEMESIS] Healing all network partitions");
        self.partitions.lock().unwrap().clear();
    }
}

#[async_trait]
impl Network for InMemoryNetwork {
    async fn send(
        &self,
        sender_id: NodeId,
        target_id: NodeId,
        message: RaftMessage,
    ) -> anyhow::Result<()> {
        if self
            .partitions
            .lock()
            .unwrap()
            .contains(&(sender_id, target_id))
        {
            return Ok(());
        }

        let sender_clone = self.senders.lock().unwrap().get(&target_id).cloned();

        if let Some(tx) = sender_clone {
            let event = Event::Rpc { sender_id, message };
            let _ = tx.send(event).await;
        }
        Ok(())
    }

    async fn run_listener(&self) -> anyhow::Result<()> {
        Ok(())
    }
}
