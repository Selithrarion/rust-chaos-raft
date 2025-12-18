use axum::{Router, routing::get};
use clap::Parser;
use prometheus::{Encoder, IntGauge, TextEncoder, register_int_gauge};
use raft_core::network::Network;
use raft_core::network::tcp::TcpNetwork;
use raft_core::node::{Event, RaftNode};
use raft_core::protocol::{ClientMessage, ClientQueryResponse, ClientResponse};
use raft_core::state::NodeId;
use raft_core::storage::Storage;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
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

static RAFT_CURRENT_TERM: OnceLock<IntGauge> = OnceLock::new();
static RAFT_COMMIT_INDEX: OnceLock<IntGauge> = OnceLock::new();
static RAFT_IS_LEADER: OnceLock<IntGauge> = OnceLock::new();

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let my_id = args.id;

    let config_str = std::fs::read_to_string(&args.config)?;

    RAFT_CURRENT_TERM.get_or_init(|| {
        register_int_gauge!("raft_current_term", "The current term of the Raft node").unwrap()
    });
    RAFT_COMMIT_INDEX.get_or_init(|| {
        register_int_gauge!("raft_commit_index", "The commit index of the Raft node").unwrap()
    });
    RAFT_IS_LEADER.get_or_init(|| {
        register_int_gauge!(
            "raft_is_leader",
            "Is this node the leader? (1 if yes, 0 if no)"
        )
        .unwrap()
    });

    let config: Config = toml::from_str(&config_str)?;

    let my_config = config
        .nodes
        .iter()
        .find(|n| n.id == my_id)
        .ok_or_else(|| anyhow::anyhow!("Node with id {} not found in config", my_id))?
        .clone();
    let peer_addresses: HashMap<NodeId, String> = config
        .nodes
        .into_iter()
        .map(|n| (n.id, n.raft_address))
        .collect();

    let data_dir = format!("./data/node_{}", my_id);
    let peers: Vec<NodeId> = peer_addresses
        .keys()
        .cloned()
        .filter(|id| *id != my_id)
        .collect();

    let storage = Arc::new(Storage::new(std::path::Path::new(&data_dir))?);

    let initial_status = raft_core::protocol::NodeStatusInfo {
        role: raft_core::state::NodeRole::Follower,
        term: 0,
        commit_index: 0,
        leader_id: None,
    };
    let (status_tx, status_rx) = tokio::sync::watch::channel(initial_status);

    let (event_tx, event_rx) = tokio::sync::mpsc::channel(128);

    let network = Arc::new(TcpNetwork::new(
        my_id,
        Arc::new(peer_addresses),
        event_tx.clone(),
    ));
    spawn_background_tasks(
        &my_config,
        event_tx.clone(),
        network.clone(),
        status_rx.clone(),
    );

    tokio::spawn(async move {
        let mut rx = status_rx;
        loop {
            if rx.changed().await.is_ok() {
                let status = rx.borrow();
                RAFT_CURRENT_TERM.get().unwrap().set(status.term as i64);
                RAFT_COMMIT_INDEX
                    .get()
                    .unwrap()
                    .set(status.commit_index as i64);
                RAFT_IS_LEADER.get().unwrap().set(
                    if status.role == raft_core::state::NodeRole::Leader {
                        1
                    } else {
                        0
                    },
                );
            }
        }
    });

    let node = RaftNode::new(my_id, peers, network, storage, status_tx, event_rx);
    node.run().await;

    Ok(())
}

async fn run_client_listener(
    address: String,
    event_tx: tokio::sync::mpsc::Sender<Event>,
    status_rx: tokio::sync::watch::Receiver<raft_core::protocol::NodeStatusInfo>,
) -> anyhow::Result<()> {
    use tokio::net::TcpListener;

    let listener = TcpListener::bind(&address).await?;
    println!("[Client Listener] Listening on {}", address);

    loop {
        let (socket, _) = listener.accept().await?;
        let event_tx = event_tx.clone();
        let status_rx_clone = status_rx.clone();

        tokio::spawn(async move {
            if let Err(e) = handle_client_connection(socket, event_tx, status_rx_clone).await {
                eprintln!("[Client Handler] Error: {}", e);
            }
        });
    }
}

async fn handle_client_connection(
    mut socket: tokio::net::TcpStream,
    event_tx: tokio::sync::mpsc::Sender<Event>,
    status_rx: tokio::sync::watch::Receiver<raft_core::protocol::NodeStatusInfo>,
) -> anyhow::Result<()> {
    use tokio::io::AsyncReadExt;

    let Ok(len) = socket.read_u32().await else {
        return Ok(());
    };

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
            let status = status_rx.borrow().clone();
            send_client_response(&mut socket, &ClientQueryResponse::Status(status)).await;
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

fn spawn_background_tasks(
    config: &NodeConfig,
    event_tx: tokio::sync::mpsc::Sender<Event>,
    network: Arc<TcpNetwork>,
    status_rx: tokio::sync::watch::Receiver<raft_core::protocol::NodeStatusInfo>,
) {
    tokio::spawn(async move {
        if let Err(e) = network.run_listener().await {
            eprintln!("[TCP Network] Raft listener failed: {}", e);
        }
    });

    let client_event_tx = event_tx.clone();
    let client_address = config.client_address.clone();
    tokio::spawn(async move {
        if let Err(e) = run_client_listener(client_address, client_event_tx, status_rx).await {
            eprintln!("[Client Listener] Failed: {}", e);
        }
    });

    tokio::spawn(async move {
        let app = Router::new().route("/metrics", get(serve_metrics));
        let listener = tokio::net::TcpListener::bind("0.0.0.0:9100").await.unwrap();
        println!("[Metrics] Listening on http://0.0.0.0:9100/metrics");
        axum::serve(listener, app).await.unwrap();
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

async fn serve_metrics() -> impl axum::response::IntoResponse {
    let encoder = TextEncoder::new();
    let mut buffer = vec![];
    let mf = prometheus::gather();
    encoder.encode(&mf, &mut buffer).unwrap();
    buffer.extend_from_slice(b"# EOF\n");
    (
        axum::http::StatusCode::OK,
        [(
            "Content-Type",
            "application/openmetrics-text; version=1.0.0; charset=utf-8",
        )],
        buffer,
    )
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
