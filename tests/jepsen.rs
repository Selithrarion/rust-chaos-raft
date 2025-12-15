use std::collections::HashSet;
use std::sync::Arc;
use tokio::time::{Duration, interval, sleep};

mod common;

#[tokio::main]
async fn main() {
    let network = Arc::new(raft_core::network::mem::InMemoryNetwork::new());
    let peers1: Vec<u64> = vec![2, 3];
    let peers2: Vec<u64> = vec![1, 3];
    let peers3: Vec<u64> = vec![1, 2];

    let tx1 = common::spawn_node(1, peers1, network.clone());
    let tx2 = common::spawn_node(2, peers2, network.clone());
    let tx3 = common::spawn_node(3, peers3, network.clone());

    let all_nodes = vec![(tx1.clone(), 1), (tx2.clone(), 2), (tx3.clone(), 3)];

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

    assert!(
        common::client_request(&leader_tx, "SET key=initial".to_string())
            .await
            .is_ok()
    );
    assert!(
        common::client_request(&leader_tx, "SET val=1".to_string())
            .await
            .is_ok()
    );

    for (_other_tx, other_id) in &all_nodes {
        if *other_id != leader_id {
            network.partition(leader_id, *other_id);
            network.partition(*other_id, leader_id);
        }
    }

    let res_old_leader = tokio::time::timeout(
        Duration::from_secs(1),
        common::client_request(&leader_tx, "SET key=partitioned".to_string()),
    )
    .await;
    assert!(
        res_old_leader.is_err(),
        "Write to partitioned leader should have failed/timed out"
    );

    sleep(Duration::from_secs(2)).await;

    let majority_nodes: Vec<_> = all_nodes
        .iter()
        .filter(|(_, id)| *id != leader_id)
        .cloned()
        .collect();
    let (new_leader_tx, _) = common::find_leader(&majority_nodes).await;
    assert!(
        common::client_request(&new_leader_tx, "SET val=2".to_string())
            .await
            .is_ok()
    );

    network.heal_all();
    sleep(Duration::from_secs(2)).await;

    for id in 1..=3 {
        let data_dir = format!("./data/test_node_{}", id);
        let storage = raft_core::storage::Storage::new(std::path::Path::new(&data_dir)).unwrap();
        let log = storage.load_log().unwrap();

        let commands: HashSet<String> = log
            .into_iter()
            .map(|e| String::from_utf8(e.command).unwrap())
            .collect();

        assert!(commands.contains("SET key=initial"));
        assert!(commands.contains("SET val=1"));
        assert!(commands.contains("SET val=2"));
        assert!(!commands.contains("SET key=partitioned"));
    }

    println!("[TEST] PASSED...");
}
