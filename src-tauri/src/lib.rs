#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use raft_core::protocol::{ClientMessage, ClientResponse, NodeStatusInfo};
use serde::Deserialize;
use serde::Serialize;
use std::collections::HashMap;
use tauri::State;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;

struct RaftClient {
    node_addresses: HashMap<u64, String>,
    sorted_node_ids: Vec<u64>,
    leader_index: usize,
}

impl RaftClient {
    async fn send_command(&mut self, command: Vec<u8>) -> anyhow::Result<ClientResponse> {
        let start_index = self.leader_index;

        for i in 0..self.sorted_node_ids.len() {
            let current_index = (start_index + i) % self.sorted_node_ids.len();
            let node_id_to_try = self.sorted_node_ids[current_index];
            let target_addr = self.node_addresses.get(&node_id_to_try).unwrap();
            // println!("[UI Client] Attempting to send command to node {}", current_leader_id);

            match TcpStream::connect(target_addr).await {
                Ok(mut stream) => {
                    let message = if command == b"NOOP" {
                        ClientMessage::NoOp
                    } else {
                        ClientMessage::Request {
                            command: command.clone(),
                        }
                    };
                    let serialized = bincode::serialize(&message)?;
                    let len = serialized.len() as u32;

                    stream.write_u32(len).await?;
                    stream.write_all(&serialized).await?;

                    if let Ok(Some(response)) =
                        read_framed_response::<ClientResponse>(&mut stream).await
                    {
                        match response {
                            ClientResponse::Success => {
                                if let Some(leader_pos) = self
                                    .sorted_node_ids
                                    .iter()
                                    .position(|&id| id == node_id_to_try)
                                {
                                    self.leader_index = leader_pos;
                                }
                                return Ok(response);
                            }
                            ClientResponse::NotLeader(_) => continue,
                        }
                    }
                }
                Err(_e) => {
                    // println!("[UI Client] Failed to connect to node {}: {}. Trying next", node_id_to_try, _e);
                    continue;
                }
            }
        }

        Err(anyhow::anyhow!(
            "Could not find a leader to process the command."
        ))
    }
}

pub struct AppState {
    client: Mutex<RaftClient>,
}

#[derive(Serialize, Clone, Copy)]
enum ConnectionStatus {
    Connected,
}

#[tauri::command]
fn ping() -> &'static str {
    "pong"
}

#[tauri::command]
fn get_connection_status() -> ConnectionStatus {
    ConnectionStatus::Connected
}

#[derive(Serialize, Clone)]
struct ClusterStatus {
    leader_id: u64,
}

#[tauri::command]
async fn get_cluster_status(state: State<'_, AppState>) -> Result<ClusterStatus, String> {
    let mut client = state.client.lock().await;
    let mut handles = Vec::new();

    for (id, addr) in &client.node_addresses {
        handles.push(tokio::spawn(query_node_status(*id, addr.clone())));
    }

    let results: Vec<Option<NodeStatusInfo>> = futures::future::join_all(handles)
        .await
        .into_iter()
        .map(|res| res.unwrap_or(None))
        .collect();

    let mut leader_id = 0;

    for status_opt in results.iter().flatten() {
        if status_opt.role == raft_core::state::NodeRole::Leader {
            if let Some(leader_pos) = client
                .sorted_node_ids
                .iter()
                .position(|&id| id == status_opt.leader_id.unwrap_or(0))
            {
                client.leader_index = leader_pos;
            }

            leader_id = status_opt.leader_id.unwrap_or(0);
        }
    }

    if leader_id != 0 {
        Ok(ClusterStatus { leader_id })
    } else {
        Err("Could not determine leader from node statuses.".to_string())
    }
}

async fn query_node_status(_id: u64, address: String) -> Option<NodeStatusInfo> {
    match tokio::time::timeout(
        std::time::Duration::from_millis(200),
        perform_query(address),
    )
    .await
    {
        Ok(Ok(result)) => result,
        Err(_) => None,
        Ok(Err(_)) => None,
    }
}

async fn perform_query(address: String) -> anyhow::Result<Option<NodeStatusInfo>> {
    let mut stream = TcpStream::connect(address).await?;
    let query = ClientMessage::GetStatus;
    let serialized = bincode::serialize(&query)?;
    let len = serialized.len() as u32;
    stream.write_u32(len).await?;
    stream.write_all(&serialized).await?;

    let response =
        read_framed_response::<raft_core::protocol::ClientQueryResponse>(&mut stream).await?;
    let Some(raft_core::protocol::ClientQueryResponse::Status(status)) = response else {
        return Ok(None);
    };
    Ok(Some(status))
}

#[derive(Deserialize)]
struct NodeConfig {
    id: u64,
    client_address: String,
    admin_address: String,
}

#[derive(Deserialize)]
struct Config {
    nodes: Vec<NodeConfig>,
}

async fn read_framed_response<T: serde::de::DeserializeOwned>(
    stream: &mut TcpStream,
) -> anyhow::Result<Option<T>> {
    let Ok(len) = stream.read_u32().await else {
        return Ok(None);
    };

    let mut buffer = vec![0; len as usize];
    let Ok(_) = stream.read_exact(&mut buffer).await else {
        return Ok(None);
    };

    bincode::deserialize(&buffer).map(Some).map_err(Into::into)
}

#[derive(Serialize, Clone)]
struct PingResult {
    alive_nodes: Vec<u64>,
}

#[tauri::command]
async fn ping_nodes(state: State<'_, AppState>) -> Result<PingResult, String> {
    let client = state.client.lock().await;
    let mut alive_nodes = Vec::new();
    for node_id in &client.sorted_node_ids {
        if let Some(address) = client.node_addresses.get(node_id) {
            let connect_future = TcpStream::connect(address);
            if tokio::time::timeout(std::time::Duration::from_millis(200), connect_future)
                .await
                .is_ok()
            {
                alive_nodes.push(*node_id);
            }
        }
    }
    Ok(PingResult { alive_nodes })
}

#[tauri::command]
async fn kill_node(node_id: u64) -> Result<(), String> {
    let config_str = std::fs::read_to_string("../raft.toml").map_err(|e| e.to_string())?;
    let config: Config = toml::from_str(&config_str).map_err(|e| e.to_string())?;
    let node_config = config
        .nodes
        .iter()
        .find(|n| n.id == node_id)
        .ok_or_else(|| format!("Node {} not found in config", node_id))?;

    let admin_addr = &node_config.admin_address;
    println!(
        "[Chaos] Attempting to kill node {} at {}",
        node_id, admin_addr
    );
    let mut stream = TcpStream::connect(&admin_addr)
        .await
        .map_err(|e| e.to_string())?;
    stream
        .write_all(b"SHUTDOWN")
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
async fn measure_recovery_time(state: State<'_, AppState>) -> Result<u128, String> {
    let cluster_status = get_cluster_status(state.clone()).await?;
    let leader_id_to_kill = cluster_status.leader_id;

    println!(
        "[Chaos] Killing leader node {} to measure recovery time",
        leader_id_to_kill
    );
    kill_node(leader_id_to_kill).await?;

    let initial_alive_count = ping_nodes(state.clone()).await?.alive_nodes.len();
    loop {
        let current_alive_count = ping_nodes(state.clone()).await?.alive_nodes.len();
        if current_alive_count < initial_alive_count {
            println!("[Chaos] Leader process confirmed dead. Starting recovery timer");
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    println!("[Chaos] Phase 2: Measuring time to elect a new leader");

    let start_time = std::time::Instant::now();
    let max_attempts = 50; // 50 * 100ms = 5 seconds timeout
    let mut attempts = 0;

    loop {
        if attempts >= max_attempts {
            return Err("Cluster did not recover in time.".to_string());
        }
        attempts += 1;

        let mut client = state.client.lock().await;
        match client.send_command(b"NOOP".to_vec()).await {
            Ok(ClientResponse::Success) => {
                let duration = start_time.elapsed();
                println!(
                    "[Chaos] Cluster recovered! New leader elected. Downtime: {}ms",
                    duration.as_millis()
                );
                return Ok(duration.as_millis());
            }
            Ok(ClientResponse::NotLeader(_)) | Err(_) => {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                continue;
            }
        }
    }
}

#[tauri::command]
async fn client_request(command: String, state: State<'_, AppState>) -> Result<(), String> {
    // println!("[UI] Sending command to cluster: {}", command);
    let result = state
        .client
        .lock()
        .await
        .send_command(command.into_bytes())
        .await;

    match result {
        Ok(ClientResponse::Success) => Ok(()),
        Ok(ClientResponse::NotLeader(_)) => Err("Command failed: Not a leader.".to_string()),
        Err(e) => Err(e.to_string()),
    }
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    let config_str = std::fs::read_to_string("../raft.toml")
        .or_else(|_| std::fs::read_to_string("raft.toml"))
        .expect("Failed to read raft.toml in tauri app");
    let config: Config =
        toml::from_str(&config_str).expect("Failed to parse raft.toml in tauri app");

    let node_addresses: HashMap<u64, String> = config
        .nodes
        .into_iter()
        .map(|n| (n.id, n.client_address))
        .collect();

    let mut sorted_node_ids: Vec<u64> = node_addresses.keys().cloned().collect();
    sorted_node_ids.sort();

    let client = RaftClient {
        node_addresses,
        sorted_node_ids,
        leader_index: 0,
    };

    tauri::Builder::default()
        .manage(AppState {
            client: Mutex::new(client),
        })
        .setup(|_app| {
            println!("Tauri GUI started. Run raft nodes separately");
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            ping,
            get_connection_status,
            get_cluster_status,
            ping_nodes,
            kill_node,
            client_request,
            measure_recovery_time
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
