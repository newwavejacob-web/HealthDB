//vote handliong, elecition logic
use crate::raft::{NodeState, Role, RequestVoteMsg, send_rpc, AppendEntriesMsg, RequestVoteResponse, AppendEntriesResponse, RaftMsg};
use crate::raft::RaftMsg::{RequestVote, AppendEntries};
use crate::raft::replication::log_replication;
use tokio::time::timeout;


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

    
    pub async fn send_heartbeats(state: &NodeState, peers: &[String]){
        // when i become a leader i have to send my Heartbeat
        // send empty append entries every like 200 ms.
        let heartbeat = RaftMsg::AppendEntries(AppendEntriesMsg {
            term: 0,
            leader_id: 0,
            prev_log_idx: 0,
            prev_log_term: 0,
            entries: Vec::new(), 
            leader_commit: 0,
        });
        // and i have to keep sending the heartbeat, or i could just do that in my main timer loop.
        // logic is kinda funky since i am directly using the select! loop to send heartbeats but
        // to be honest it checks out. it works out really cleanly with my code cause select lets
        // it so that if no special rpcs get sent but theres a leader we send a heartbeat to all.
        for peer in peers {
            let hb = heartbeat.clone();
            let p = peer.clone(); 
            tokio::spawn(async move {
                let _ = send_rpc(&p, hb).await;
            });
            if state.role == Role::Leader {
                log_replication(mut state);
            }
        }
    }

    

