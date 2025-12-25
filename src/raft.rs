// this is wherer im gonna start building raft shit is gonna be lit'
use std::fs::File;
use serde::{Serialize, Deserialize};

/* raft mesaging */

// our in house per node storage
struct NodeState {
    role: Role,
    currentTerm: u64,
    votedFor: Option<u64>,
              
    log: File, 
         
    //volState
    commit_index: u64,
    last_applied: u64,

    //leader VolState
    next_index: u64,
    match_index: u64,
}
    
enum Role {
    Leader,
    Follower,
    Candidate,
}

#[derive(Serialize, Deserialize, Debug)]
struct AppendEntriesMsg {
    term: u64,
    leader_id: u64,
    prev_log_idx: u64,
    prev_log_term_entries: Vec<u64>, 
    leader_commit: u64,
}

#[derive(Serialize, Deserialize, Debug)]
struct RequestVoteMsg {
    term: u64,
    candidate_id: u64,
    last_log_idx: u64, 
    last_log_term: u64,
}

#[derive(Serialize, Deserialize, Debug)]
struct RequestVoteResponse {
    votedFor: Option<u64>,
    currentTerm: u64,
    voted: bool,
}

#[derive(Serialize, Deserialize)]
struct AppendEntriesResponse {
    currentTerm: u64,
    success: bool,
}

#[derive(Serialize, Deserialize)]
enum RaftMsg {
    AppendEntries(AppendEntriesMsg),
    RequestVote(RequestVoteMsg),
    AppendEntriesResponse(AppendEntriesResponse),
    RequestVoteResponse(RequestVoteResponse),
}
//invoked by leader
pub fn append_entries(term: u64, leader_id: u64, prev_log_idx: u64, prev_log_term_entries: Vec<u64>, leader_commit: u64) -> bool { //term, success return variables
    if term < currentTerm {
        return false;
    }
    if log[prev_log_idx] != prev_log_term_entries[what index??]{
        return false;
    }
    // if log entries conflict, we have to delete the existing entry and all that follow 

    for log_idx in log {
        if prev_log_idx == log_idx {
            // delete logic here
            // literally just deleting the file tho. iterate until we delete all after the entry
        }
    }  


    logs::append_log(); // create new log shit. i need them the same not one for both delete and
    logs::log::set;

    if leader_commit > commit_index {
        let commit_index = min(leader_commit, last_applied /* index of last new entry, wouldnt that be last applied*/);
    }

}
// we arent actually voting here, we are checking logs to get a "vote"
pub fn request_vote(term: u64, candidate_id: u64, last_log_idx: u64, last_log_term: u64) ->  bool // currentTerm, voteGranted
{
    if term < currentTerm {
        return false;
    }
    if candidate_id == 0 {
        return true;
    }
    if candidate_id != 0 {
        // here is our actualy logic, we have to check to see if the candidate log is as up to date
        // as the recievers log. 
        // 
        // what is our up to date logic again? we just update it haere to grant vote

        return true;
    }
}




//data flow. 
//deserialize incoming bytes (into rpc structs)
//do raft shit
//serialize outcoming byte

/* --- COMMUNICATION LAYER HERE --- */

//USE SERDE

// send rpc
#[test]
#[should_panic]
pub fn send_node_msg (node_id: &str, msg_type: RaftMsg,) {
    let mut stream = TcpStream::connect(node_id).unwrap();
    // write info to stream here, this is our basis. we gonnna be basically just sending serde
    // right.
    match msg_type {
        "AppendEntriesMsg" => {
            let msg = RaftMsg::AppendEntriesMsg
            send_append_msg(&msg);
        }
        "AppendEntriesResponse" => {
            let msg = RaftMsg::AppendEntriesResponse
            send_append_resp(&msg);
        }
        "RequestVoteMsg" => {
            let msg = RaftMsg::RequestVoteMsg
            send_vote_msg(&msg);
        }
        "RequestVoteResponse" => {
            let msg = RaftMsg::RequestVoteResponse
            send_vote_resp(&msg);
        }
        _ => eprintln!("not valid raft RPC")   
    }
}
//ik  what its supposed to look like 
//serde everything into nodes.
//pass nodes everywhere
//if node is leader, then send heartbeat every like fucking 200 ms idk.
pub fn heartbeat(node_id: &str){
        let mut stream = TcpStream::connect(node_id).unwrap();
        let heartbeat_msg = AppendEntriesMsg {
            term,
            leader_id,
            prev_log_idx: 0,
            prev_log_term_entries: vec![],
            leader_commit: 0,
        };
        let stream_write = bincode::serialize(&heartbeat_msg).unwrap();

        let mut buffer = [0u8; 1024];
        let read = stream.read(&mut buffer).unwrap();
        println!("revieved H34RTB33T {:?} ", &buffer[..read]);
        
        stream.write_all(stream_write);
    
    // sleep(Duration::from_millis(300));    
    // maybe i should put this in the read? have it so that it reads every 300 ms and if it doesnt
    // detect a heartbeat tbhen it automatically transitions in to candidate status 
    
}
/*
pub fn send_append_msg(){

}
pub fn send_append_resp(){
}
pub fn send_vote_msg(){
}
pub fn send_vot_resp(){
}
*/
pub fn detect_heartbeat(node_id: &str) {
    let listener = TcpListener::bind(node_id).unwrap();

    for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    let mut stream = stream.unwrap();
                    let mut buffer = [0u8; 1024];
                    let read = stream.read(&mut buffer).unwrap();

                    let stream_read = bincode::deserialize(&read).unwrap();
                    if stream_read == AppendEntriesMsg.isEmpty() {
                        let NodeState::role = Follower;
                        sleep(Duration::from_millis(300));
                    } 
                    else if stream_read != AppendEntriesMsg {
                        let NodeState::role = Candidate;
                        // become candidate?
                        // then we gotta vote for oour selves andstart leader elevtion. 
                        start_election(node);
                    }
                    
                }
                Err(e) => {
                    eprintln!("no tcp connection detected");
                }
            }
    }
}
pub fn start_election(node: Node, node_state: NodeState){
    node::self.current_term += 1;
    node::self.voted_for = Some(self.node_id);
    let mut votes = 1;

    for node in nodes {
        if vote_success && response.term = self.current_term {
            votes += 1;

            if votes > (num_nodes / 2) {
                let Node::role = Leader;
                self.become_leader();
            }
        }
    }
}
// here, if we are a follower, and we havent revieved a heartbeat in 200ms, become candidate
/*pub fn leader_election(){
    if Role::Follower {
        if Node::noHeartBeat {
            Node::Role = Candidate;
            // then we start voting shit. just need this for the heartbeat.
        }
    }
}*/
pub fn recieve_node_msg (node_id: &str) {
    let listener = TcpListener::bind(node_id).unwrap();
    
    for stream in listener.incoming() {
            match stream {
                Ok(stream) => {

                    let mut buffer = [0u8; 1024];
                    let read = stream.read(&buffer[..read]).unwrap(); 
                    let msg: RaftMsg = bincode::deserialize(&buffer).unwrap();

                        // DO DIFFERENT LOGIC DEPENDING ON WHAT WE REVIECE.
                        // CALL FUNCTIONS here
                        match msg {
                            RaftMsg::AppendEntriesMsg(ae) => {
                        
                            }
                            RaftMsg::RequestVoteMsg(rv) => {

                            }
                            RaftMsg::AppendEntriesResponse(resp) => {

                            }
                            RaftMsg::RequestVoteResponse(resp) => {

                            }
                        }

                    });
                }
                Err(e) => eprintln!("Connection failed: {}", e),
            }
        
    }
}
