//TODO I HAVE TO FIGURE OUT THE LEADER PART OF APPEND ENTRIES
     /* whenever we call a client request to the database we append to WAL whenever that happens we append to our leader log and start log_replication followers recieve the properly log to update from leader, and respond back 
     * once majority has replicated an entry, we advance the commit index.
     * once last applied works and every node as replicated commited entries, apply them to the
     * state machine
     */
use std::collections::HashMap;
use crate::raft::{NodeState, Role, LogEntry, RaftMsg, AppendEntriesMsg, broadcast, apply_entry};
use crate::store::Database;

    // here im gonna actually write the fault tolerant logs to Database
    pub fn write_to_logs(state: &mut NodeState, peer: String, matched_to: u64){
        state.match_index.insert(peer.to_string(), matched_to);
        state.next_index.insert(peer.to_string(), matched_to + 1);

        advance_commit(state);
    }
    // this is where we check to see if we can advance our commit
    pub fn advance_commit(state: &mut NodeState){
        if state.role != Role::Leader { return; }

        for n in (state.commit_index + 1)..=(state.log.len() as u64) {
            let mut match_count = 1;
            let idx = (n - 1) as usize;

            if state.log[idx].term != state.current_term {
                continue;
            }

            for (_peer, &matched) in &state.match_index {
                if matched >= n {
                    match_count += 1;
                }
            }

            let majority = (state.peers.len() + 1)/ 2 + 1;

            if match_count >= majority && state.log[idx].term == state.current_term {
                state.commit_index = n;
            }
        }
    }
// how do i implement concurrent handling 
    pub async fn log_replication(state: &mut NodeState) {
        let mut requests = Vec::new();
        // how far each peer's log will match ours if it accepts this message
        let mut sent_up_to = HashMap::new();

        for peer in state.peers.clone() {
            let next_idx = *state.next_index.get(&peer).unwrap_or(&1);
            let prev_idx = next_idx - 1;
            let prev_term = if prev_idx > 0 {
                state.log.get((prev_idx - 1) as usize).map(|e| e.term).unwrap_or(0)
            } else {
                0
            };

            let entries: Vec<LogEntry> = state.log
                .iter()
                .skip((next_idx - 1) as usize)
                .cloned()
                .collect();
            sent_up_to.insert(peer.clone(), prev_idx + entries.len() as u64);

            let msg = RaftMsg::AppendEntries(AppendEntriesMsg {
                term: state.current_term,
                leader_id: state.node_id,
                prev_log_idx: prev_idx, 
                prev_log_term: prev_term,
                entries, 
                leader_commit: state.commit_index,
            });
            requests.push((peer, msg));
        }

        for (peer, result) in broadcast(requests).await {
            match result {
                Ok(RaftMsg::AppendEntriesResponse(response)) => {
                    if response.current_term > state.current_term {
                        // a newer leader exists, stop acting as leader
                        state.current_term = response.current_term;
                        state.role = Role::Follower;
                        state.voted_for = None;
                        state.persist_meta();
                        return;
                    }
                    if response.success {
                        write_to_logs(state, peer.clone(), sent_up_to[&peer]);
                    }
                    else {
                        state.next_index.insert(peer.clone(), response.next_index);
                    }
                }
                Err(e) => eprintln!("Error: {}", e),
                _ => {}
            }
        }
        // commit_index may have moved
        state.persist_meta();
    }
    
    // applies newly-committed entries to the store. runs on every node, so followers
    // (whose commit_index advances inside handle_append_entries) apply them too.
    pub fn apply_committed(state: &mut NodeState, db: &Database) {
        while state.commit_index > state.last_applied {
            state.last_applied += 1;
            apply_entry(db, &state.log[(state.last_applied - 1) as usize]);
        }
    }

/*pub fn apply_entry(db: &mut Database, entry: &LogEntry){
     let data_str = String::from_utf8_lossy(&entry.data);
     let parts: Vec<&str> = data_str.split_whitespace().collect();

     if parts.len() < 3 { return; }
     let key = parts[3];

     match parts[1].to_uppercase().as_str() {
        "SET" => {
            if parts.len() >= 4 {
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
}*/                                                                                                                                                                                                                     
