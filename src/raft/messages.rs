//RaftMsg, RequestVote, AppendEntries
//always remember to modulate. it will help you think about the problem clearer
use crate::raft::{NodeState, Role, send_rpc};
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AppendEntriesMsg {
    pub term: u64,
    pub leader_id: u64,
    pub prev_log_idx: u64,
    pub prev_log_term: u64,
    pub entries: Vec<LogEntry>, 
    pub leader_commit: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RequestVoteMsg {
    pub term: u64,
    pub candidate_id: u64,
    pub last_log_idx: u64, 
    pub last_log_term: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RequestVoteResponse {
    pub current_term: u64,
    pub voted: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AppendEntriesResponse {
    pub current_term: u64,
    pub success: bool,
}
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LogEntry {
    pub term: u64,
    pub data: Vec<u8>,
}
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum RaftMsg {
    AppendEntries(AppendEntriesMsg),
    RequestVote(RequestVoteMsg),
    AppendEntriesResponse(AppendEntriesResponse),
    RequestVoteResponse(RequestVoteResponse),
}

pub fn handle_request_vote(state: &mut NodeState, req: RequestVoteMsg) -> RequestVoteResponse{
    if req.term < state.current_term {
        return RequestVoteResponse {
            current_term: state.current_term,
            voted: false,
        };
    }

    if req.term > state.current_term {
        state.current_term = req.term;
        state.voted_for = None;
        state.role = Role::Follower;
    }

    let last_term = state.log.last().map(|e| e.term).unwrap_or(0);
    let last_idx = state.log.len() as u64;

    let can_vote = state.voted_for.is_none() || state.voted_for == Some(req.candidate_id);
    let up_to_date_check = req.last_log_term == last_term && req.last_log_idx >= last_idx;

    if can_vote && up_to_date_check {
        state.voted_for = Some(req.candidate_id);
        return RequestVoteResponse {
            current_term: state.current_term,
            voted: true,
        };
    }

    RequestVoteResponse {
        current_term: state.current_term,
        voted: false,
    }
}

// this is our logic for how our follower nodes respond to the Append Entries msg.
pub fn handle_append_entries(state: &mut NodeState, req: AppendEntriesMsg) -> AppendEntriesResponse{
    if req.term > state.current_term {
        state.current_term = req.term;
        state.role = Role::Follower;
        state.voted_for = None;
    }
    //1
    if req.term < state.current_term {
        return AppendEntriesResponse {
            term: state.current_term,
            success: false,
        };
    }
    //2
    if req.prev_log_idx > 0 {
        let idx = (req.prev_log_idx - 1) as usize;
        if state.log.len() <= idx || state.log[prev_log_idx].term != req.prev_log_term {
            return AppendEntriesResponse {
                term: state.current_term,
                success: false,
            };
        }
    }
    /*
    for i in state.log.len() {
        if i >= state.leader_commit {
            state.log.remove(i);
        } 
    } */
    //3 and 4 
    let start_index = req.prev_log_idx as usize;
    state.log.truncate(start_index);
    state.log.extend(req.entries);

    //5
    if req.leader_commit > state.commit_index {
        state.commit_index = min(req.leader_commit, state.log.len() as u64);
    }

    AppendEntriesResponse {
        current_term: state.current_term,
        success: true,
    }
}

pub fn handle_messages(state: &mut NodeState, msg: RaftMsg) -> RaftMsg {
    match msg {
        RaftMsg::AppendEntries(ae) => {
            if state.role == Role::Candidate {
                state.role = Role::Follower;
            }
            RaftMsg::AppendEntriesResponse(handle_append_entries(state, ae))
        }
        RaftMsg::RequestVote(rv) => {
            RaftMsg::RequestVoteResponse(handle_request_vote(state,rv))
        }
        _ => msg
    }
}
