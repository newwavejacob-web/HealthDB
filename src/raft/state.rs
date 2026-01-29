// this is basically our rule book, they sshow thsi shti ib the algorithm
// NOdeState, Role
use crate::raft::messages::LogEntry;
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq)]
pub enum Role {
    Leader,
    Follower,
    Candidate,
}

// our in house per node storage
pub struct NodeState {
    pub role: Role,
    pub current_term: u64,
    pub voted_for: Option<u64>,
              
    pub log: Vec<LogEntry>, // is this even waht we want it as or do we want it as a vec of structs
         
    //volState
    pub commit_index: u64,
    pub last_applied: u64,

    //leader VolState
    pub next_index: HashMap<String, u64>,
    pub match_index: HashMap<String, u64>,
    
    pub node_id: u64,
    pub address: String,
    pub peers: Vec<String>,
}

impl NodeState {
    pub fn new(node_id: u64, address: String, peers: Vec<String>) -> NodeState {
        NodeState {
            role: Role::Follower,
            current_term: 0,
            voted_for: None,
            log: Vec::new(), // have to figure this out.
            commit_index: 0,
            last_applied: 0,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            node_id,
            address,
            peers,
        }
    }
}
