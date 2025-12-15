use crate::message::RaftMessage;
use crate::network::Network;
use crate::node::Event;
use crate::protocol::WireMessage;
use crate::state::NodeId;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, mpsc};

pub struct TcpNetwork {
    id: NodeId,
    peer_addresses: Arc<HashMap<NodeId, String>>,
    event_tx: mpsc::Sender<Event>,
    // TODO: maybe better to use tokio tasks and mpsc
    connections: Arc<Mutex<HashMap<NodeId, BufWriter<TcpStream>>>>,
}

impl TcpNetwork {
    pub fn new(
        id: NodeId,
        peer_addresses: Arc<HashMap<NodeId, String>>,
        event_tx: mpsc::Sender<Event>,
    ) -> Self {
        Self {
            id,
            peer_addresses,
            event_tx,
            connections: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

#[async_trait]
impl Network for TcpNetwork {
    async fn send(
        &self,
        sender_id: NodeId,
        target_id: NodeId,
        message: RaftMessage,
    ) -> anyhow::Result<()> {
        let mut connections = self.connections.lock().await;

        if !connections.contains_key(&target_id) {
            if let Some(address) = self.peer_addresses.get(&target_id) {
                match TcpStream::connect(address).await {
                    Ok(stream) => {
                        connections.insert(target_id, BufWriter::new(stream));
                    }
                    Err(e) => {
                        return Err(e.into());
                    }
                }
            }
        }

        if let Some(stream) = connections.get_mut(&target_id) {
            let wire_msg = WireMessage { sender_id, message };
            let serialized = bincode::serialize(&wire_msg)?;
            let len = serialized.len() as u32;

            if stream.write_u32(len).await.is_err()
                || stream.write_all(&serialized).await.is_err()
                || stream.flush().await.is_err()
            {
                connections.remove(&target_id);
                return Err(anyhow::anyhow!(
                    "Failed to write to stream, connection closed"
                ));
            }
        }
        Ok(())
    }

    async fn run_listener(&self) -> anyhow::Result<()> {
        let my_address = self.peer_addresses.get(&self.id).unwrap();
        let listener = TcpListener::bind(my_address).await?;
        println!("[TCP Network] Node {} listening on {}", self.id, my_address);

        loop {
            let (mut socket, _address) = listener.accept().await?;

            let event_tx = self.event_tx.clone();
            tokio::spawn(async move {
                if let Ok(len) = socket.read_u32().await {
                    let mut buffer = vec![0; len as usize];
                    if socket.read_exact(&mut buffer).await.is_ok() {
                        if let Ok(wire_msg) = bincode::deserialize::<WireMessage>(&buffer) {
                            let event = Event::Rpc {
                                sender_id: wire_msg.sender_id,
                                message: wire_msg.message,
                            };
                            if event_tx.send(event).await.is_err() {
                                eprintln!(
                                    "[TCP Network] Failed to send received message to Raft actor: channel closed."
                                );
                            }
                        }
                    }
                }
            });
        }
    }
}
