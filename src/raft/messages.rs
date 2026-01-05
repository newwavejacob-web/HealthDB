//RaftMsg, RequestVote, AppendEntries
//always remember to modulate. it will help you think about the problem clearer
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AppendEntriesMsg {
    term: u64,
    leader_id: u64,
    prev_log_idx: u64,
    prev_log_term_entries: Vec<u64>, 
    leader_commit: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RequestVoteMsg {
    term: u64,
    candidate_id: u64,
    last_log_idx: u64, 
    last_log_term: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RequestVoteResponse {
    votedFor: Option<u64>,
    currentTerm: u64,
    voted: bool,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct AppendEntriesResponse {
    currentTerm: u64,
    success: bool,
}
#[derive(Serialize, Deserialize, Clone)]
pub struct LogEntry {
    term: u64,
    data: Vec<u8>,
}
#[derive(Serialize, Deserialize, Clone)]
pub enum RaftMsg {
    AppendEntries(AppendEntriesMsg),
    RequestVote(RequestVoteMsg),
    AppendEntriesResponse(AppendEntriesResponse),
    RequestVoteResponse(RequestVoteResponse),
}
/*
pub fn send_node_msg (&self, node_id: &str, msg_type: RaftMsg,) {
    let mut stream = TcpStream::connect(node_id).unwrap();
    // write info to stream here, this is our basis. we gonnna be basically just sending serde
    match msg_type {
        // so look here we are literally saying to match on MSG FUCKING TYPE, so we have to
        // fucking match on this type dumbass
        RaftMsg::AppendEntries(ae) =>{
            todo!();
        }
        RaftMsg::AppendEntriesResponse(resp) =>{
            todo!();
        }
        RaftMsg::RequestVote(rv) => {
            todo!();
        }
        RaftMsg::RequestVoteResponse(resp) => {
            todo!();
        }
        _ => eprintln!("not valid raft RPC")   
    }
}
pub fn recieve_node_msg (&self, node_id: &str) {
    let listener = TcpListener::bind(node_id).unwrap();
    
    for stream in listener.incoming() {
        match stream {

            Ok(stream) => {
                let mut buffer = [0u8; 1024];
                let read = stream.read(&mut buffer).unwrap(); 
                let msg: RaftMsg = bincode::deserialize(&buffer[..read]).unwrap();

                    // DO DIFFERENT LOGIC DEPENDING ON WHAT WE REVIECE.
                    // CALL FUNCTIONS here
                    match msg {
                        RaftMsg::AppendEntries(ae) => {
                            todo!();
                        }
                        RaftMsg::RequestVote(rv) => {
                            todo!();
                        }
                        RaftMsg::AppendEntriesResponse(resp) => {
                            todo!();
                        }
                        RaftMsg::RequestVoteResponse(resp) => {
                            todo!();
                        }
                    }
            }

            Err(e) => eprintln!("Connection failed: {}", e),
        }
    }
}
pub fn handle_request_vote(&mut self, req: RequestVoteMsg) -> RequestVoteResponse{
    if req.term < self.current_term {
        return RequestVoteResponse {
            current_term: self.current_term,
            voted: false,
        };
    }

    if req.term > self.current_term {
        self.current_term = req.term;
        self.voted_for = None;
        self.role = Role::Follower;
    }

    let last_term = self.log.last().map(|e| e.term).unwrap_or(0);
    let last_idx = self.log.len() as u64;

    let can_vote = self.voted_for.is_none() || self.voted_for == Some(req.candidate_id);
    let up_to_date_check = req.last_log_term == last term && req.last_log_idx >= last_idx;

    if can_vote && up_to_date_check {
        self.voted_for = Some(req.candidate_id);
        return RequestVoteResponse {
            current_term: self.current_term,
            voted: true,
        };
    }

    RequestVoteResponse {
        current_term: self.current_term,
        voted: false,
    }

}
pub fn handle_append_entries(&mut self, req: AppendEntriesMsg) -> AppendEntriesResponse{
    // send heartbeats unless we wanna do log replication

    AppendEntriesResponse {

    }
}*/
