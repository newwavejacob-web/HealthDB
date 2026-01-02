// this is basically our rule book, they sshow thsi shti ib the algorithm
// NOdeState, Role


enum Role {
    Leader,
    Follower,
    Candidate,
}

// our in house per node storage
struct NodeState {
    role: Role,
    current_term: u64,
    voted_for: Option<u64>,
              
    log: Vec<u64>, // is this even waht we want it as or do we want it as a vec of structs
         
    //volState
    commit_index: u64,
    last_applied: u64,

    //leader VolState
    next_index: u64,
    match_index: u64,
    
    node_id: String,
    peers: Vec<String>,
}
pub async fn state_rules(&self, mpsc::Sender<RaftEvent>, mpsc::Reciever<RaftEvent>){
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
}
