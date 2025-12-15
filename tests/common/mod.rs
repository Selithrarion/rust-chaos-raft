use raft_core::network::mem::InMemoryNetwork;
use raft_core::node::{Event, RaftNode};
use raft_core::storage::Storage;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{Duration, sleep};

pub fn spawn_node(id: u64, peers: Vec<u64>, network: Arc<InMemoryNetwork>) -> mpsc::Sender<Event> {
    let (event_tx, event_rx) = mpsc::channel(128);
    network.add_node(id, event_tx.clone());

    let data_dir = format!("./data/test_node_{}", id);
    let _ = std::fs::remove_dir_all(&data_dir);
    let storage = Arc::new(Storage::new(std::path::Path::new(&data_dir)).unwrap());

    let node = RaftNode::new(id, peers, network.clone(), storage, event_rx);
    tokio::spawn(node.run());

    event_tx
}

pub async fn client_request(
    leader_tx: &mpsc::Sender<Event>,
    command: String,
) -> anyhow::Result<()> {
    let (resp_tx, resp_rx) = oneshot::channel();
    leader_tx
        .send(Event::ClientCommand {
            command: command.into_bytes(),
            responder: resp_tx,
        })
        .await?;
    resp_rx.await?
}

pub async fn find_leader(nodes: &[(mpsc::Sender<Event>, u64)]) -> (mpsc::Sender<Event>, u64) {
    for _ in 0..20 {
        for (tx, id) in nodes {
            let (resp_tx, resp_rx) = oneshot::channel();
            if tx
                .send(Event::GetStatus { responder: resp_tx })
                .await
                .is_ok()
            {
                if let Ok(status) = resp_rx.await {
                    if status.role == raft_core::state::NodeRole::Leader {
                        return (tx.clone(), *id);
                    }
                }
            }
        }
        sleep(Duration::from_millis(100)).await;
    }
    panic!("Could not find a leader in the cluster.");
}
