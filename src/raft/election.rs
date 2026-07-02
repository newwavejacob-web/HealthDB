//vote handliong, elecition logic
use crate::raft::{NodeState, Role, RequestVoteMsg, send_rpc, RaftMsg};
use crate::raft::RaftMsg::RequestVote;
use crate::raft::replication::log_replication;


    pub async fn start_leader_election(state: &mut NodeState) {
        state.current_term += 1;
        state.voted_for = Some(state.node_id.clone());
        
        let mut votes = 1;
        let majority = (state.peers.len() + 1) / 2 + 1;

        for peer in state.peers.clone() {
            let request = RequestVote(RequestVoteMsg {
                term: state.current_term,
                candidate_id: state.node_id,
                last_log_idx: state.log.len() as u64,
                last_log_term: state.log.last().map(|e| e.term).unwrap_or(0),
            });

            let req = request.clone();

            match send_rpc(&peer, req).await {
                Ok(RaftMsg::RequestVoteResponse(response)) => {
                    if response.current_term > state.current_term {
                        state.role = Role::Follower;
                        state.current_term = response.current_term;
                    }
                    if response.voted && response.current_term == state.current_term {
                        votes += 1;
                        if votes >= majority {
                            state.role = Role::Leader;
                        }
                    }
                    state.next_index.insert(peer.clone(), state.log.len() as u64 + 1);
                    state.match_index.insert(peer.clone(), 0);
                }
                Err(e) => eprintln!("Error: {}", e),
                _ => {}
            }
        }
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

    

