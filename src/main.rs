// GOAL IS TO GET A LOCK FREE VERISON WORKING by the end of 2025

/*mod store;
mod server;
mod clients;
mod logs; */
mod raft;

use raft::NodeState;

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();

    if args.len() < 2 {
        println!("Usage: cargo run <node id>");
        return;
    }

    let node_id: u64 = args[1].parse().unwrap();
    let my_port = 5000 + node_id; 
    let addr = format!("127.0.0.1:{}", my_port);

    let peers: Vec<String> = (1..=3)
        .filter(|&id| id != node_id)
        .map(|id| format!("127.0.0.1:{}", 5000 + id))
        .collect();

    let state = NodeState::new(node_id, addr.clone(), peers);

    println!("Node {} on {}", node_id, addr);
    println!("Peers: {:?}", state.peers);
   /* let db = store::new();
    logs::create_log(&db);
    server::run(db); */

}
