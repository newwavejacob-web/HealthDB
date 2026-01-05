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
    pub next_index: u64,
    pub match_index: u64,
    
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
            next_index: 0,
            match_index: 0,
            node_id,
            address,
            peers,
        }
    }
}
// basically everything we are coding is one big state machine
/*pub async fn state_rules(&self, mpsc::Sender<RaftEvent>, mpsc::Reciever<RaftEvent>){
    if commit_index > last_applied {
        last_applied += 1;
        logs::append_log(log[last_applied]);
    }

    match self.state {
        node::follower => {

        }
        node::candidate => {
            for node in nodes {
                start_election(node,node_state);
            }
            //start election, send request_vode rpc
            //count number of responses, based off our up to date and rpc rules,
            //thats our vote, if larger than cluster size, we become leader,
            //if we see a higher term, become follower
        }
        node::leader => {
            if term >= current_term {
                Node::become_follower;
            }
            send_heartbeat();
//                log_replication();
            // send empty Append Entries rpc to keep followers alive
            // send not empty Append entries for log replication, thats the fun part.
            // if it sees higher term, becomes follower, new leader election
        }
    }
}*/
