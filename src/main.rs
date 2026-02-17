mod store;
mod server;
mod clients;
mod logs; 
mod raft;

use raft::{NodeState, Role, RaftMsg, RequestVoteMsg, send_rpc, read_rpc, write_rpc, start_leader_election, handle_messages, send_heartbeats, apply_entry};
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

    println!("Node {} on {}", node_id, addr);
    println!("Peers: {:?}", state.peers);

    let listener_addr = addr.clone();

    let listener = TcpListener::bind(&listener_addr).await.unwrap();

    
    let client_listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    // TODO I HAVE TO WIRE UP WAL TO RAFT, THIS IS JUST RAFT STILL.
        tokio::time::sleep(Duration::from_secs(20));


        let election_timeout = Duration::from_millis(150 + (node_id * 50));
        // but i have to send multipe heartbeats and send them concurrently to check when one goes
         while state.commit_index > state.last_applied {
                state.last_applied += 1;
                let entry = &state.log[(state.last_applied - 1) as usize];
                apply_entry(&db, entry);
        }

        loop {
            tokio::select! {
                // this sleep block needs to eventually be our random election timeout 
                _ = sleep(election_timeout) => {
                    println!("Election Timeout, Starting leader election");
                    state.role = Role::Candidate;
                    let p_clone = state.peers.clone();
                    start_leader_election(&mut state).await;
                }
                // sends heartbeats if we have a leader every 125 ms, lowkey works really well with
                // tokio select! macro 
                _ = sleep(Duration::from_millis(125)), if state.role == Role::Leader => {
                    let p_clone = state.peers.clone();
                    send_heartbeats(&mut state, &p_clone).await;
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



