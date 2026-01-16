//TODO I HAVE TO FIGURE OUT THE LEADER PART OF APPEND ENTRIES
     /* whenever we call a client request to the database we append to WAL whenever that happens we append to our leader log and start log_replication followers recieve the properly log to update from leader, and respond back 
     * once majority has replicated an entry, we advance the commit index.
     * once last applied works and every node as replicated commited entries, apply them to the
     * state machine
     */

    // here im gonna actually write the fault tolerant logs to Database
    pub fn write_to_logs(state: &mut NodeState, peer: &str, matched_to: u64){
        state.match_index.insert(peer.to_string(), matched_to);
        state.next_index.insert(peer.to_string(), matched_to + 1);

        advance_commit(state);
    }
    // this is where we check to see if we can advance our commit
    pub fn advance_commit(state: &mut NodeState){
        if state.role != Role::Leader { return; }

        for n in (state.commit_index + 1)..=(state.log.len() as u64) {
            let mut match_count = 1;
            for (_peer, &matched) in &state.match_index {
                if matched >= n {
                    match_count += 1;
                }
            }

            let majority = (state.peers.len() + 1)/ 2 + 1;
            let idx = (n - 1) as usize;

            if match_count >= majority && state.log[idx].term == state.current_term {
                state.commit_index = n;
            }
        }
    }
    pub async fn log_replication(state: &mut NodeState, log_append: Vec<LogEntry>) {
        for peer in state.peers {
            let next_idx = state.next_index.get(peer);
            let match_idx = state.match_index.get(peer);

            let msg = RaftMsg::AppendEntries(AppendEntriesMsg {
                term: state.current_term,
                leader_id: state.node_id,
                prev_log_idx: next_idx - 1,
                prev_log_term: state.log[next_idx].term,
                entries: log_append, 
                leader_commit: state.commit_index,
            });
                match send_rpc(&peer, msg).await {
                    Ok(RaftMsg::AppendEntriesResponse(response)) => {
                        if response.success {
                            write_to_logs(state, peer, match_idx );
                        }
                        if !response.success {
                            next_idx -= 1;
                            state.next_index.delete(peer);
                            state.next_index.insert(peer, next_idx);
                        }
                    }
                    Err(e) => eprintln!("Error: {}", e),
                    _ => {}
                }
        }
    }
