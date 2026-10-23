//RaftMsg, RequestVote, AppendEntries
//always remember to modulate. it will help you think about the problem clearer
use crate::raft::{NodeState, Role};
use serde::{Serialize, Deserialize};
use crate::store::{self, Database};
use crate::raft::persist::Snapshot;

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
    pub next_index: u64,
}
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LogEntry {
    pub term: u64,
    pub data: Vec<u8>,
}
// sent instead of AppendEntries when a follower needs entries the leader has compacted.
// the whole snapshot goes in one message; fine for this store's size, no chunking yet.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct InstallSnapshotMsg {
    pub term: u64,
    pub leader_id: u64,
    pub snapshot: Snapshot,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct InstallSnapshotResponse {
    pub current_term: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum RaftMsg {
    AppendEntries(AppendEntriesMsg),
    RequestVote(RequestVoteMsg),
    AppendEntriesResponse(AppendEntriesResponse),
    RequestVoteResponse(RequestVoteResponse),
    InstallSnapshot(InstallSnapshotMsg),
    InstallSnapshotResponse(InstallSnapshotResponse),
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

    let last_term = state.last_term();
    let last_idx = state.last_index();

    let can_vote = state.voted_for.is_none() || state.voted_for == Some(req.candidate_id);
    // Raft §5.4.1: candidate's log is at least as up-to-date if its last term is higher,
    // or the terms tie and its log is at least as long.
    let up_to_date_check = req.last_log_term > last_term
        || (req.last_log_term == last_term && req.last_log_idx >= last_idx);

    if can_vote && up_to_date_check {
        state.voted_for = Some(req.candidate_id);
        state.heard_from_leader = true;
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
pub fn handle_append_entries(state: &mut NodeState, mut req: AppendEntriesMsg) -> AppendEntriesResponse{
    if req.term > state.current_term {
        state.current_term = req.term;
        state.role = Role::Follower;
        state.voted_for = None;
    }
    //1
    if req.term < state.current_term {
        return AppendEntriesResponse {
            current_term: state.current_term,
            success: false,
            next_index: state.last_index() + 1,
        };
    }
    // the sender is the current leader, even if its log doesn't line up with ours yet
    state.heard_from_leader = true;

    // entries up to our snapshot are committed, so we already have them. skip ahead.
    if req.prev_log_idx < state.snapshot_index {
        let skip = (state.snapshot_index - req.prev_log_idx) as usize;
        req.entries.drain(..skip.min(req.entries.len()));
        req.prev_log_idx = state.snapshot_index;
        req.prev_log_term = state.snapshot_term;
    }

    //2
    match state.term_at(req.prev_log_idx) {
        None => {
            // our log is too short, ask for what comes after our last entry
            return AppendEntriesResponse {
                current_term: state.current_term,
                success: false,
                next_index: state.last_index() + 1,
            };
        }
        Some(term) if term != req.prev_log_term => {
            // skip back over the whole conflicting term in one round trip
            let mut conflict_idx = req.prev_log_idx;
            while conflict_idx > state.snapshot_index + 1 && state.term_at(conflict_idx - 1) == Some(term) {
                conflict_idx -= 1;
            }
            return AppendEntriesResponse {
                current_term: state.current_term,
                success: false,
                next_index: conflict_idx,
            };
        }
        _ => {}
    }
    
    //3 and 4 
    // skip entries we already have, drop ours from the first conflict on, append the rest
    for (i, entry) in req.entries.iter().enumerate() {
        let index = req.prev_log_idx + 1 + i as u64;

        if state.term_at(index) == Some(entry.term) {
            continue;
        }
        state.truncate_from(index);
        state.append_entries(&req.entries[i..]);
        break;
    }

    //5
    if req.leader_commit > state.commit_index {
        state.commit_index = std::cmp::min(req.leader_commit, state.last_index());
    }

    AppendEntriesResponse {
        current_term: state.current_term,
        success: true,
        next_index: state.last_index() + 1,
    }
}

// leader sent its snapshot because we're behind the start of its log
pub fn handle_install_snapshot(state: &mut NodeState, req: InstallSnapshotMsg, db: &Database) -> InstallSnapshotResponse {
    if req.term > state.current_term {
        state.current_term = req.term;
        state.role = Role::Follower;
        state.voted_for = None;
    }
    if req.term < state.current_term {
        return InstallSnapshotResponse { current_term: state.current_term };
    }
    state.heard_from_leader = true;

    // we already have everything this covers, committed, in our own log or snapshot
    if req.snapshot.last_index <= state.commit_index {
        return InstallSnapshotResponse { current_term: state.current_term };
    }

    state.install_snapshot(&req.snapshot);
    *db.lock().unwrap() = req.snapshot.data;
    InstallSnapshotResponse { current_term: state.current_term }
}

pub fn handle_messages(state: &mut NodeState, msg: RaftMsg, db: &Database) -> RaftMsg {
    let response = match msg {
        RaftMsg::AppendEntries(ae) => {
            if state.role == Role::Candidate {
                state.role = Role::Follower;
            }
            RaftMsg::AppendEntriesResponse(handle_append_entries(state, ae))
        }
        RaftMsg::RequestVote(rv) => {
            RaftMsg::RequestVoteResponse(handle_request_vote(state,rv))
        }
        RaftMsg::InstallSnapshot(snap) => {
            if state.role == Role::Candidate {
                state.role = Role::Follower;
            }
            RaftMsg::InstallSnapshotResponse(handle_install_snapshot(state, snap, db))
        }
        _ => msg
    };
    // whatever term/vote/commit we're about to report must already be durable
    state.persist_meta();
    response
}

//pub fn handle_append_response(state: &mut NodeState, msg: AppendEntriesResponse) {
    
pub fn apply_entry(db: &Database, entry: &LogEntry){
     let data_str = String::from_utf8_lossy(&entry.data);
     let parts: Vec<&str> = data_str.split_whitespace().collect();

     if parts.len() < 3 { return; }

     match parts[1].to_uppercase().as_str() {
        "SET" => {
            if parts.len() >= 4 {
                let key = parts[2].to_string();
                let value = parts[3..parts.len()-1].join(" ");
                store::set(db, key, value, false);
            }
        }
        "DEL" => {
            let key = parts[2];
            store::delete(db, key, false);
        }
        _ => {}
     }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::test_dir;

    #[test]
    fn test_append_entries_valid() {
        let mut state = NodeState::new(1, "127.0.0.1:5001".into(), vec![], test_dir("ae-valid"));
        state.current_term = 1;

        let req = AppendEntriesMsg {
            term: 1,
            leader_id: 2,
            prev_log_idx: 0,
            prev_log_term: 0,
            entries: vec![LogEntry { term: 1, data: b"SET foo bar".to_vec() }],
            leader_commit: 0,
        };

        let resp = handle_append_entries(&mut state, req);
        assert!(resp.success);
        assert_eq!(state.log.len(), 1);
    }

    #[test]
    fn test_append_entries_rejects() {
        let mut state = NodeState::new(1, "127.0.0.1:5001".into(), vec![], test_dir("ae-rejects"));
        state.current_term = 5;

        let req = AppendEntriesMsg {
            term: 3,
            leader_id: 2,
            prev_log_idx: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
        };

        let resp = handle_append_entries(&mut state, req);
        assert!(!resp.success);
    }
}
