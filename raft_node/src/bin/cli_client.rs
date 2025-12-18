use raft_core::protocol::{ClientMessage, ClientResponse};
use serde::Deserialize;
use std::collections::HashMap;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[derive(Deserialize, Debug)]
struct NodeConfig {
    id: u64,
    client_address: String,
}

#[derive(Deserialize, Debug)]
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let command = std::env::args()
        .nth(1)
        .expect("Usage: cli_client \"<COMMAND>\"");
    println!("[Client] Sending command: '{}'", command);

    let config_str = std::fs::read_to_string("raft.toml")?;
    let config: Config = toml::from_str(&config_str)?;

    let node_addresses: HashMap<u64, String> = config
        .nodes
        .into_iter()
        .map(|n| (n.id, n.client_address))
        .collect();

    for node_id in node_addresses.keys() {
        let target_addr = node_addresses.get(node_id).unwrap();
        println!("[Client] Trying node {} at {}...", node_id, target_addr);

        match TcpStream::connect(target_addr).await {
            Ok(mut stream) => {
                let message = ClientMessage::Request {
                    command: command.clone().into_bytes(),
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
                            println!(
                                "[Client] Success! Command processed by leader (Node {}).",
                                node_id
                            );
                            return Ok(());
                        }
                        ClientResponse::NotLeader(_) => {
                            println!(
                                "[Client] Node {} is not the leader. Trying next...",
                                node_id
                            );
                            continue;
                        }
                    }
                }
            }
            Err(e) => {
                println!(
                    "[Client] Failed to connect to node {}: {}. Trying next...",
                    node_id, e
                );
                continue;
            }
        }
    }

    Err(anyhow::anyhow!(
        "Could not find a leader to process the command."
    ))
}
