use std::sync::Arc;
use tokio::time::{Duration, interval, sleep};

mod common;

#[tokio::main]
async fn main() {
    let network = Arc::new(raft_core::network::mem::InMemoryNetwork::new());
    let tx1 = common::spawn_node(1, vec![2, 3], network.clone());
    let tx2 = common::spawn_node(2, vec![1, 3], network.clone());
    let tx3 = common::spawn_node(3, vec![1, 2], network.clone());

    let all_nodes = vec![(tx1, 1), (tx2, 2), (tx3.clone(), 3)];

    let ticker_nodes = all_nodes.clone();
    tokio::spawn(async move {
        let mut ticker = interval(Duration::from_millis(50));
        loop {
            ticker.tick().await;
            for (tx, _) in &ticker_nodes {
                let _ = tx.send(raft_core::node::Event::Tick).await;
            }
        }
    });

    let (leader_tx, leader_id) = common::find_leader(&all_nodes).await;

    let follower_to_isolate = all_nodes
        .iter()
        .find(|(_, id)| *id != leader_id)
        .expect("Should be at least one follower to isolate")
        .clone();
    let (_follower_tx, follower_id) = follower_to_isolate;

    println!("[TEST] Isolating follower Node {}", follower_id);
    for (_tx, id) in &all_nodes {
        if *id != follower_id {
            network.partition(follower_id, *id);
            network.partition(*id, follower_id);
        }
    }

    println!("[TEST] Generating load to trigger snapshot...");
    let leader_tx_clone = leader_tx.clone();
    tokio::spawn(async move {
        for i in 0..15 {
            let command = format!("SET key{}={}", i, i);
            let _ = common::client_request(&leader_tx_clone, command).await;
        }
    });

    sleep(Duration::from_secs(1)).await;
    println!("[TEST] Snapshot should be created by now");

    println!(
        "[TEST] Healing partition, Node {} should recover from snapshot",
        follower_id
    );
    network.heal_all();

    sleep(Duration::from_secs(5)).await;

    println!("[TEST] Verifying state machine consistency");
    let mut final_states = Vec::new();

    //     println!("[DEBUG] Listing contents of ./data/ directory:");
    //     if let Ok(entries) = fs::read_dir("./data") {
    //         for entry in entries {
    //             if let Ok(entry) = entry {
    //                 println!("  - {:?}", entry.path());
    //             }
    //         }
    //     }

    for id in 1..=3 {
        let data_dir = format!("./data/test_node_{}", id);
        let storage = raft_core::storage::Storage::new(std::path::Path::new(&data_dir)).unwrap();
        let snapshot_data = storage.load_snapshot().unwrap_or_else(|e| {
            panic!("[TEST] Failed to load snapshot for node {}: {}", id, e);
        });

        let (_metadata, state_machine) =
            snapshot_data.unwrap_or_else(|| panic!("[TEST] Snapshot is missing for node {}", id));

        final_states.push(state_machine);
    }

    let first_state = &final_states[0];
    assert!(
        final_states.iter().all(|s| s == first_state),
        "State machines are not consistent"
    );

    println!("[TEST] Snapshot recovery test PASSED!");
}
