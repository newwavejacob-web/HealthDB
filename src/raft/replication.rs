//TODO I HAVE TO FIGURE OUT THE LEADER PART OF APPEND ENTRIES
     /* whenever we call a client request to the database we append to WAL whenever that happens we append to our leader log and start log_replication followers recieve the properly log to update from leader, and respond back 
     * once majority has replicated an entry, we advance the commit index.
     * once last applied works and every node as replicated commited entries, apply them to the
     * state machine
     */
use std::collections::HashMap;
use crate::raft::{NodeState, Role, RaftMsg, AppendEntriesMsg, InstallSnapshotMsg, broadcast, apply_entry};
use crate::raft::persist::{Snapshot, read_snapshot};
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

        for n in (state.commit_index + 1)..=state.last_index() {
            // only entries from our own term are committed by counting replicas (§5.4.2)
            if state.term_at(n) != Some(state.current_term) {
                continue;
            }

            let mut match_count = 1;
            for (_peer, &matched) in &state.match_index {
                if matched >= n {
                    match_count += 1;
                }
            }

            let majority = (state.peers.len() + 1)/ 2 + 1;

            if match_count >= majority {
                state.commit_index = n;
            }
        }
    }
// how do i implement concurrent handling 
    pub async fn log_replication(state: &mut NodeState) {
        let mut requests = Vec::new();
        // how far each peer's log will match ours if it accepts this message
        let mut sent_up_to = HashMap::new();
        let mut snapshot: Option<Snapshot> = None;

        for peer in state.peers.clone() {
            let next_idx = *state.next_index.get(&peer).unwrap_or(&1);

            // the entries this peer needs next have been compacted away, send the snapshot
            if next_idx <= state.snapshot_index {
                if snapshot.is_none() {
                    snapshot = Some(read_snapshot(&state.snapshot_path()).expect("fatal: could not read snapshot"));
                }
                let snap = snapshot.clone().unwrap();
                sent_up_to.insert(peer.clone(), snap.last_index);
                requests.push((peer, RaftMsg::InstallSnapshot(InstallSnapshotMsg {
                    term: state.current_term,
                    leader_id: state.node_id,
                    snapshot: snap,
                })));
                continue;
            }

            let prev_idx = next_idx - 1;
            let prev_term = state.term_at(prev_idx).unwrap_or(0);
            let entries = state.entries_from(next_idx);
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

        for (peer, result) in broadcast(requests, state.tls.clone()).await {
            let (term, success, next_index) = match result {
                Ok(RaftMsg::AppendEntriesResponse(r)) => (r.current_term, r.success, r.next_index),
                Ok(RaftMsg::InstallSnapshotResponse(r)) => (r.current_term, true, 0),
                Err(e) => { eprintln!("Error: {}", e); continue; }
                _ => continue,
            };
            if term > state.current_term {
                // a newer leader exists, stop acting as leader
                state.current_term = term;
                state.role = Role::Follower;
                state.voted_for = None;
                state.persist_meta();
                return;
            }
            if success {
                write_to_logs(state, peer.clone(), sent_up_to[&peer]);
            }
            else {
                state.next_index.insert(peer.clone(), next_index.max(1));
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
            apply_entry(db, state.entry(state.last_applied));
        }

        if state.last_applied - state.snapshot_index >= state.snapshot_threshold {
            let data = db.lock().unwrap().clone();
            state.compact(data);
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
