/// actually storage operations??
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use crate::logs;

/*pub struct Database {
    db: Arc<Mutex<HashMap<String, String>>>,// isLoggable: bool,} */
                                            //
pub type Database = Arc<Mutex<HashMap<String, String>>>;

//impl Database {
pub fn new() -> Database {
    Arc::new(Mutex::new(HashMap::new()))
}

pub fn set(db: &Database, key: String, value: String, log_flag: bool) {
    println!("store::set called with log_flag: {}", log_flag);
    if log_flag {
        replication::leader_write(logs::log_set(&key, &value).unwrap());
    }
        let mut map = db.lock().unwrap();
        map.insert(key, value);
}
//option return type is how we get some and none if get works or not
pub fn get(db: &Database, key: &str) -> Option<String> {
    println!("store::get called");
    let map = db.lock().unwrap();
    map.get(key).cloned() 
}
pub fn delete(db: &Database, key: &str, log_flag: bool) -> bool {
    println!("store::del called with log_flag: {}", log_flag);
    if log_flag {
        replication::leader_write(logs::log_del(&key));
    }
        let mut map = db.lock().unwrap();
        map.remove(key).is_some()
}
//}
/ actually storage operations??
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use crate::logs;

/*pub struct Database {
    db: Arc<Mutex<HashMap<String, String>>>,// isLoggable: bool,} */
                                            //
pub type Database = Arc<Mutex<HashMap<String, String>>>;

//impl Database {
pub fn new() -> Database {
    Arc::new(Mutex::new(HashMap::new()))
}

pub fn set(db: &Database, key: String, value: String, log_flag: bool) {
    println!("store::set called with log_flag: {}", log_flag);
    if log_flag {
        replication::leader_write(logs::log_set(&key, &value).unwrap());
    }
        let mut map = db.lock().unwrap();
        map.insert(key, value);
}
//option return type is how we get some and none if get works or not
pub fn get(db: &Database, key: &str) -> Option<String> {
    println!("store::get called");
    let map = db.lock().unwrap();
    map.get(key).cloned() 
}
pub fn delete(db: &Database, key: &str, log_flag: bool) -> bool {
    println!("store::del called with log_flag: {}", log_flag);
    if log_flag {
        replication::leader_write(logs::log_del(&key));
    }
        let mut map = db.lock().unwrap();
        map.remove(key).is_some()
}
//}

mod store;
mod server;
mod clients;
mod logs; 
mod raft;

use raft::{NodeState, Role, RaftMsg, RequestVoteMsg, send_rpc, read_rpc, write_rpc, start_leader_election, handle_messages, send_heartbeats};
use tokio::net::TcpListener;
use tokio::time::{Duration, sleep};

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

    let mut state = NodeState::new(node_id, addr.clone(), peers);

    println!("Node {} on {}", node_id, addr);
    println!("Peers: {:?}", state.peers);

    let listener_addr = addr.clone();

    let listener = TcpListener::bind(&listener_addr).await.unwrap();

    // start testing leader election,, fucking working on modulating too
    //if state.peers.len() <= 3 {
        /*
        //Heartbeat Test
        if node_id == 1 {
            state.role = Role::Leader; 
            loop{
                tokio::time::sleep(Duration::from_millis(150)).await;
                match send_heartbeats(&state, peers) {
                    Ok(_) => println!("Election Timer Reset");
                    Err(e) => println!("Error: {}", e);
                }
            }
        }
        */

        let election_timeout = Duration::from_millis(150 + (node_id * 50));
        // but i have to send multipe heartbeats and send them concurrently to check when one goes
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
                    send_heartbeats(&state, &p_clone).await;
                }
                // this accounts for both normal rpc calls as well as heartbeats
                result = listener.accept() => {
                    let (mut stream, _ ) = result.unwrap();
                    if let Ok(msg) = read_rpc(&mut stream).await {
                        let response = handle_messages(&mut state, msg);
                        write_rpc(&mut stream, response).await;
                    }
                } 
            }
            while state.commit_index > state.last_applied {
                state.last_applied += 1;
                let entry = &state.log[(state.last_applied - 1) as usize];
                reload(&db, entry);
            }
            if state.role == Role::Leader {
                for i in state.log {
                    // but how do i handle the "majority of match_index[i] >= i " case?
                    if state.log[i].term == state.current_term && i > state.commit_index {
                        state.commit_index = i;
                        return;
                    }
                }
            }
        }
        
//    }

    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
   /* let db = store::new();
    logs::create_log(&db);
    server::run(db); */

}

