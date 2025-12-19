// this is wherer im gonna start building raft shit is gonna be lit'
// so how do i even start and go about this.

struct perState{
    currentTerm: i64,
    votedFor: //TcpStream??
              // or wait does this shit just go on the WAL

}

struct VolState{
    commit_index: i64,
    last_applied: i64,

    //leader VolState
    next_index: i64,
    match_index: i64,
}

pub fn append_entries(term: i64, leader_id: i64, prev_log_idx: i64, prev_log_term_entries: /*array??*/ , leader_commit: i64) -> i64, bool { //term, success return variables

}
