// this is wherer im gonna start building raft shit is gonna be lit'


/* raft mesaging */
struct perState{
    currentTerm: i64,
    votedFor: //TcpStream??
              
    log: // this is our wal
         
    //volState
    commit_index: i64,
    last_applied: i64,

    //leader VolState
    next_index: i64,
    match_index: i64,
}
enum RPC {
    append_entries, request_vote,
}

//invoked by leader
pub fn append_entries(term: i64, leader_id: i64, prev_log_idx: i64, prev_log_term_entries: /*array??*/ , leader_commit: i64) -> i64, bool { //term, success return variables
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
pub fn request_vote(term: i64, candidate_id: i64, last_log_idx: i64, last_log_term: i64) -> i64, bool // currentTerm, voteGranted
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

pub fn send_node_msg () {}
pub fn recieve_node_msg () {}
