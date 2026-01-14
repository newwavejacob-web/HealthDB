mod raft;

#[tokio::test]
async fn main(){
    // so how do we test our raft code?
    // spawn some nodes and try to get em talking??
    
    let addr1 = "127.0.0.1:5001";
    let addr2 = "127.0.0.1:5002";
    let addr3 = "127.0.0.1:5003";

    let peers = vec![
        addr1.to_string();
        addr2.to_string();
        addr3.to_string();
    ]

        let args: Vec<string> = std::env::args().collect();
        let my_port = &args[1];

        let addr = format!("127.0.0.1:{}", my_port);

        //spawn listener: (rpc_handler()), and try sending a message
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_append_entries_valid(){
        let mut state = NodeState::new(1, "127.0.0.1:5001".into(), vec![]);
        state.current_term = 1;
        
        // i could have this be crafted directly in the clients.rs file
        let req = AppendEntriesMsg {
            term: 1,
            leader_id: 2,
            prev_log_idx: 0,
            prev_log_term: 0,
            entries: vec![LogEntry { term: 1, data: b"SET foo bar". to_vec()}],
            leader_commit: 0,
        };

        let resp = handle_append_entries(&mut state, req);
        assert!(resp.success);
        assert_eq!(state.log.len(), 1);
    }
    
    #[test]

    fn test_append_entries_rejects(){
        let mut state = NodeState::new(1, "127.0.0.1:5001".into(), vec![]);
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
