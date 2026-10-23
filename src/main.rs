mod store;
mod server;
mod clients;
mod logs; 
mod raft;

use raft::{NodeState, Role, read_rpc_with_timeout, write_rpc, start_leader_election, handle_messages, send_heartbeats, apply_committed};
use std::path::PathBuf;
use tokio::net::TcpListener;
use tokio::time::{Duration, sleep};
use tokio::io::{AsyncBufReadExt, BufReader, AsyncWriteExt};
use clients::parse_command;


#[tokio::main]
async fn main() {
    let db = store::new();
    let args: Vec<String> = std::env::args().collect();

    if args.len() < 2 {
        println!("Usage: cargo run -- <node id> [--nodes <cluster size>] [--data-dir <dir>]");
        return;
    }

    let node_id: u64 = args[1].parse().unwrap();
    // cluster size comes from --nodes (default 5). node ids run 1..=N and each node's
    // raft port is 5000 + id, so every node can work out its peers from N alone.
    let cluster_size: u64 = flag_value(&args, "--nodes").map(|n| n.parse().unwrap()).unwrap_or(5);
    if node_id < 1 || node_id > cluster_size {
        println!("node id must be between 1 and {}", cluster_size);
        return;
    }
    let my_port = 5000 + node_id; 
    let addr = format!("127.0.0.1:{}", my_port);

    let peers: Vec<String> = (1..=cluster_size)
        .filter(|&id| id != node_id)
        .map(|id| format!("127.0.0.1:{}", 5000 + id))
        .collect();

    // crash recovery: reload term, vote and raft log from disk, then replay the
    // committed part of the log into the in-memory store.
    let data_dir = PathBuf::from(flag_value(&args, "--data-dir").unwrap_or("."));
    std::fs::create_dir_all(&data_dir).unwrap();
    let mut state = NodeState::load(node_id, addr.clone(), peers, data_dir).expect("failed to load raft state");
    apply_committed(&mut state, &db);
    println!("Recovered term {}, {} log entries, {} committed", state.current_term, state.log.len(), state.commit_index);

    println!("Node {} on {}", node_id, addr);
    println!("Peers: {:?}", state.peers);

    let listener_addr = addr.clone();

    let listener = TcpListener::bind(&listener_addr).await.unwrap();

    
    // each node needs its own client port so multiple nodes can run on one host.
    // only the leader actually accepts client connections (see the select! arm below),
    // but every node still has to bind a distinct port.
    let client_port = 6379 + node_id;
    let client_listener = TcpListener::bind(format!("127.0.0.1:{}", client_port)).await.unwrap();
    println!("Client port: {}", client_port);

        let election_timeout = Duration::from_millis(150 + (node_id * 50));

        loop {
            tokio::select! {
                // this sleep block needs to eventually be our random election timeout 
                _ = sleep(election_timeout) => {
                    println!("Election Timeout, Starting leader election");
                    state.role = Role::Candidate;
                    start_leader_election(&mut state).await;
                }
                // sends heartbeats if we have a leader every 125 ms, lowkey works really well with
                // tokio select! macro 
                _ = sleep(Duration::from_millis(125)), if state.role == Role::Leader => {
                    send_heartbeats(&mut state).await;
                }
                // this accounts for both normal rpc calls as well as heartbeats
                result = listener.accept() => {
                    let (mut stream, _ ) = result.unwrap();
                    if let Ok(msg) = read_rpc_with_timeout(&mut stream).await {
                        let response = handle_messages(&mut state, msg);
                        if let Err(e) = write_rpc(&mut stream, response).await {
                            eprintln!("Error replying to rpc: {}", e);
                        }
                    }
                } 
                client_result = client_listener.accept(), if state.role == Role::Leader => {
                    let (stream, _) = client_result.unwrap();
                    let mut reader = BufReader::new(stream);
                    let mut line = String::new();

                    if reader.read_line(&mut line).await.is_ok() {
                        let response = parse_command(&mut state, line.trim(), &db).await;
                        let mut stream = reader.into_inner();
                        stream.write_all(response.as_bytes()).await.unwrap();
                        stream.write_all(b"\n").await.unwrap();
                    }
                }
            }

            // apply any newly-committed entries to the state machine after every event
            apply_committed(&mut state, &db);
        }
        
//    }

    /*loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    let db = store::new();
    logs::create_log(&db);
    server::run(db); 
*/
}

// returns the value after a flag, e.g. flag_value(args, "--nodes") for `--nodes 5`
fn flag_value<'a>(args: &'a [String], flag: &str) -> Option<&'a str> {
    args.iter().position(|a| a == flag).and_then(|i| args.get(i + 1)).map(|s| s.as_str())
}
