//RaftMsg, RequestVote, AppendEntries
//always remember to modulate. it will help you think about the problem clearer
//
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
