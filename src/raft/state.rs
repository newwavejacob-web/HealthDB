// this is basically our rule book, they sshow thsi shti ib the algorithm
// NOdeState, Role
use crate::raft::messages::LogEntry;

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
//TODO I HAVE TO FIGURE OUT THE LEADER PART OF APPEND ENTRIES
todo!()
    /*
     * whenever we call a client request to the database we append to WAL
     * whenever that happens we append to our leader log and start log_replication
     * followers recieve the properly log to update from leader, and respond back 
     * once majority has replicated an entry, we advance the commit index.
     * once last applied works and every node as replicated commited entries, apply them to the
     * state machine
     */

    // here im gonna actually write the fault tolerant logs to Database
    pub fn write_to_logs(state: &mut NodeState, peer: &str, matched_to: u64){
        state.match_index.insert(peer.to_string(), matched_to);
        state.next_index.insert(peer.to_string(), matched_to + 1);

        advance_commit(state);
    }
    // this is where we check to see if we can advance our commit
    pub fn advance_commit(state: &mut NodeState){
        
    }
