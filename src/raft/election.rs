//vote handliong, elecition logic
use crate::raft::{NodeState, Role, RequestVoteMsg, broadcast, RaftMsg};
use crate::raft::RaftMsg::RequestVote;
use crate::raft::replication::log_replication;
use crate::logs;


    pub async fn start_leader_election(state: &mut NodeState) {
        state.current_term += 1;
        state.voted_for = Some(state.node_id.clone());
        // the new term and our self-vote must be on disk before anyone hears about them
        state.persist_meta();
        
        let mut votes = 1;
        let majority = (state.peers.len() + 1) / 2 + 1;

        let request = RequestVote(RequestVoteMsg {
            term: state.current_term,
            candidate_id: state.node_id,
            last_log_idx: state.last_index(),
            last_log_term: state.last_term(),
        });
        let requests = state.peers.iter().map(|peer| (peer.clone(), request.clone())).collect();

        for (_peer, result) in broadcast(requests, state.tls.clone()).await {
            match result {
                Ok(RaftMsg::RequestVoteResponse(response)) => {
                    if response.current_term > state.current_term {
                        // someone is ahead of us, so this election is over
                        state.role = Role::Follower;
                        state.current_term = response.current_term;
                        state.voted_for = None;
                        state.persist_meta();
                        return;
                    }
                    if response.voted && response.current_term == state.current_term {
                        votes += 1;
                        if votes >= majority && state.role != Role::Leader {
                            state.role = Role::Leader;
                            println!("Node {} became leader for term {}", state.node_id, state.current_term);
                        }
                    }
                }
                Err(e) => eprintln!("Error: {}", e),
                _ => {}
            }
        }

        if state.role == Role::Leader {
            become_leader(state);
        }
    }

    // reset per-peer replication state and append a no-op for the new term. a leader can only
    // commit entries from its own term (Raft §5.4.2), so without the no-op, entries the old
    // leader committed stay invisible on this node until a client happens to write (Raft §8).
    fn become_leader(state: &mut NodeState) {
        for peer in state.peers.clone() {
            state.next_index.insert(peer.clone(), state.last_index() + 1);
            state.match_index.insert(peer, 0);
        }
        state.append_entries(&[logs::log_noop(state.current_term)]);
    }

    
    pub async fn send_heartbeats(state: &mut NodeState){
        // A heartbeat is just an AppendEntries RPC. log_replication already builds the correct
        // per-peer AppendEntries with the real term, leader_id, prev_log info, and any entries
        // the follower is missing. When a follower is fully caught up the entries vec is empty,
        // which is exactly a heartbeat. This keeps followers from timing out AND replicates,
        // instead of the old all-zero message that followers rejected outright.
        if state.role == Role::Leader {
            log_replication(state).await;
        }
    }

    

