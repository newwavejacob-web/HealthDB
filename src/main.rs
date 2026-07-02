mod store;
mod server;
mod clients;
mod logs; 
mod raft;

use raft::{NodeState, Role, read_rpc, write_rpc, start_leader_election, handle_messages, send_heartbeats, apply_entry};
use tokio::net::TcpListener;
use tokio::time::{Duration, sleep};
use tokio::io::{AsyncBufReadExt, BufReader, AsyncWriteExt};
use clients::parse_command;


#[tokio::main]
async fn main() {
    let db = store::new();
    let args: Vec<String> = std::env::args().collect();

    if args.len() < 2 {
        println!("Usage: cargo run <node id>");
        return;
    }

    let node_id: u64 = args[1].parse().unwrap();
    let my_port = 5000 + node_id; 
    let addr = format!("127.0.0.1:{}", my_port);

    // these are just hard coded peers. which is ok for now but how do we get this working. 
    let peers: Vec<String> = (1..=3)
        .filter(|&id| id != node_id)
        .map(|id| format!("127.0.0.1:{}", 5000 + id))
        .collect();

    let mut state = NodeState::new(node_id, addr.clone(), peers);

    // replay this node's WAL into the in-memory store on startup (crash recovery).
    logs::create_log(&db, node_id);

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
                    if let Ok(msg) = read_rpc(&mut stream).await {
                        let response = handle_messages(&mut state, msg);
                        write_rpc(&mut stream, response).await;
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

            // apply any newly-committed entries to the state machine. this runs after every
            // event so followers (whose commit_index advances inside handle_append_entries)
            // actually apply committed entries to their store, not just the leader.
            while state.commit_index > state.last_applied {
                state.last_applied += 1;
                let entry = state.log[(state.last_applied - 1) as usize].clone();
                apply_entry(&db, &entry);
            }
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



