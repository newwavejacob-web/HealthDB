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
    pub async fn log_replication(state: &mut NodeState) {
        for peer in state.peers {
            let next_idx = *state.next_index.get(&peer).unwrap_or(&1);
            let prev_idx = next_idx - 1;
            let prev_term = if prev_idx > 0 {
                state.log.get((prev_idx - 1) as usize).map(|e| e.term).unwrap_or(0);
            } else {
                0
            };

            let entries: Vec<LogEntry> = state.log
                .iter()
                .skip((next_idx - 1) as usize)
                .cloned()
                .collect();

            let msg = RaftMsg::AppendEntries(AppendEntriesMsg {
                term: state.current_term,
                leader_id: state.node_id,
                prev_log_idx: prev_idx, 
                prev_log_term: prev_term,
                entries, 
                leader_commit: state.commit_index,
            });

            match send_rpc(&peer, msg).await {
                Ok(RaftMsg::AppendEntriesResponse(response)) => {
                    if response.success {
                        write_to_logs(state, peer.clone(), state.log.len() as u64);
                    }
                    if !response.success {
                        state.next_index.insert(peer.clone(), response.next_index);
                    }
                }
                Err(e) => eprintln!("Error: {}", e),
                _ => {}
            }
        }
    }
    
    // this actually appends our nodestate log to file.
    pub async fn persist_entry(/*state: NodeState,*/ log: &LogEntry) -> std::io::Result<()> {
        //if state.role != Role::Leader { return; } 
        let mut file = OpenOptions::new()
            .append(true)
            .create(true)
            .open("log.txt")?;
        
        //state.log.push(log.clone());
        file.write_all(&log.data)?;
        println!("Appended to log");
        Ok(())
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
