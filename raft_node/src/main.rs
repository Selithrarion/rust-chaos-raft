use clap::Parser;
use raft_core::network::Network;
use raft_core::network::tcp::TcpNetwork;
use raft_core::node::{Event, RaftNode};
use raft_core::protocol::{ClientMessage, ClientQueryResponse, ClientResponse};
use raft_core::state::NodeId;
use raft_core::storage::Storage;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long)]
    id: u64,

    #[arg(long, default_value = "raft.toml")]
    config: String,
}

#[derive(Deserialize, Debug)]
struct Config {
    nodes: Vec<NodeConfig>,
}

#[derive(Deserialize, Debug, Clone)]
struct NodeConfig {
    id: NodeId,
    raft_address: String,
    client_address: String,
    admin_address: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let my_id = args.id;

    let config_str = std::fs::read_to_string(&args.config)?;
    let config: Config = toml::from_str(&config_str)?;

    let my_config = config
        .nodes
        .iter()
        .find(|n| n.id == my_id)
        .ok_or_else(|| anyhow::anyhow!("Node with id {} not found in config", my_id))?
        .clone();
    let peers: Vec<NodeId> = config
        .nodes
        .iter()
        .map(|n| n.id)
        .filter(|id| *id != my_id)
        .collect();
    let peer_addresses: HashMap<NodeId, String> = config
        .nodes
        .into_iter()
        .map(|n| (n.id, n.raft_address))
        .collect();

    let data_dir = format!("./data/node_{}", my_id);
    let storage = Arc::new(Storage::new(std::path::Path::new(&data_dir))?);

    let (event_tx, event_rx) = tokio::sync::mpsc::channel(128);

    let network = Arc::new(TcpNetwork::new(
        my_id,
        Arc::new(peer_addresses),
        event_tx.clone(),
    ));
    spawn_background_tasks(&my_config, event_tx.clone(), network.clone());

    let node = RaftNode::new(my_id, peers, network, storage, event_rx);
    node.run().await;

    Ok(())
}

async fn run_client_listener(
    address: String,
    event_tx: tokio::sync::mpsc::Sender<Event>,
) -> anyhow::Result<()> {
    use tokio::net::TcpListener;

    let listener = TcpListener::bind(&address).await?;
    println!("[Client Listener] Listening on {}", address);

    loop {
        let (socket, _) = listener.accept().await?;
        let event_tx = event_tx.clone();

        tokio::spawn(async move {
            if let Err(e) = handle_client_connection(socket, event_tx).await {
                eprintln!("[Client Handler] Error: {}", e);
            }
        });
    }
}

async fn handle_client_connection(
    mut socket: tokio::net::TcpStream,
    event_tx: tokio::sync::mpsc::Sender<Event>,
) -> anyhow::Result<()> {
    use tokio::io::AsyncReadExt;

    let len = socket.read_u32().await?;
    let mut buffer = vec![0; len as usize];
    socket.read_exact(&mut buffer).await?;

    let message: ClientMessage = bincode::deserialize(&buffer)?;

    match message {
        ClientMessage::Request { command } => {
            let response = process_write_request(command, &event_tx).await;
            send_client_response(&mut socket, &response).await;
        }
        ClientMessage::NoOp => {
            let response = process_write_request(b"NOOP".to_vec(), &event_tx).await;
            send_client_response(&mut socket, &response).await;
        }
        ClientMessage::GetStatus => {
            if let Some(response) = process_status_request(&event_tx).await {
                send_client_response(&mut socket, &response).await;
            }
        }
    }
    Ok(())
}

async fn process_write_request(
    command: Vec<u8>,
    event_tx: &tokio::sync::mpsc::Sender<Event>,
) -> ClientResponse {
    let (responder_tx, responder_rx) = tokio::sync::oneshot::channel();
    let event = Event::ClientCommand {
        command,
        responder: responder_tx,
    };
    if event_tx.send(event).await.is_ok() {
        match responder_rx.await {
            Ok(Ok(())) => ClientResponse::Success,
            _ => ClientResponse::NotLeader(None),
        }
    } else {
        ClientResponse::NotLeader(None)
    }
}

async fn process_status_request(
    event_tx: &tokio::sync::mpsc::Sender<Event>,
) -> Option<ClientQueryResponse> {
    let (responder_tx, responder_rx) = tokio::sync::oneshot::channel();
    let event = Event::GetStatus {
        responder: responder_tx,
    };
    if event_tx.send(event).await.is_ok() {
        if let Ok(status_info) = responder_rx.await {
            return Some(ClientQueryResponse::Status(status_info));
        }
    }
    None
}

fn spawn_background_tasks(
    config: &NodeConfig,
    event_tx: tokio::sync::mpsc::Sender<Event>,
    network: Arc<TcpNetwork>,
) {
    tokio::spawn(async move {
        if let Err(e) = network.run_listener().await {
            eprintln!("[TCP Network] Raft listener failed: {}", e);
        }
    });

    let client_event_tx = event_tx.clone();
    let client_address = config.client_address.clone();
    tokio::spawn(async move {
        if let Err(e) = run_client_listener(client_address, client_event_tx).await {
            eprintln!("[Client Listener] Failed: {}", e);
        }
    });

    let admin_address = config.admin_address.clone();
    tokio::spawn(async move {
        if let Err(e) = run_admin_listener(admin_address).await {
            eprintln!("[Admin Listener] Failed: {}", e);
        }
    });
}

async fn send_client_response<T: serde::Serialize>(
    socket: &mut tokio::net::TcpStream,
    response: &T,
) {
    if let Ok(serialized) = bincode::serialize(response) {
        let len = serialized.len() as u32;
        if socket.write_u32(len).await.is_ok() && socket.write_all(&serialized).await.is_ok() {
            // ok
        }
    }
}

async fn run_admin_listener(addr: String) -> anyhow::Result<()> {
    use tokio::io::AsyncReadExt;
    use tokio::net::TcpListener;

    let listener = TcpListener::bind(&addr).await?;
    println!("[Admin Listener] Listening on {}", addr);

    loop {
        let (mut socket, _) = listener.accept().await?;
        let mut buf = [0; 8];
        if socket.read_exact(&mut buf).await.is_ok() {
            if &buf == b"SHUTDOWN" {
                println!("[Admin Listener] Received SHUTDOWN command. Exiting");
                std::process::exit(0);
            }
        }
    }
}
