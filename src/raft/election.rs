//vote handliong, elecition logic
/*
    pub async fn election_timer(&self, event_tx: mpsc::Sender<RaftEvent>, reset_rx: mpsc::Receiver<RaftEvent::Heartbeat>){
        loop {
            tokio::select! {
                // this sleep block needs to eventually be our random election timeout 
                _ = sleep(Duration::from_millis(300)) => {
                    println("Election start, no reset");
                    //or 
                    //event_tx.send(RaftEvent::ElectionTimeout).await;
                }
                _ = reset_rx.recv() => {
                    println!("election reset due to leader");
                    //reset loop here
                } 
            }
        }
    }
    pub async fn start_leader_election(&mut self) {
        loop {
            self.role = Role::Candidate;
            self.current_term += 1;
            self.voted_for = Some(self.node_id.clone());
            
            let mut votes = 1;
            let majority = (peers.len() + 1) / 2 + 1;

            let (tx, mut rx) = mpsc::channell(peers.len());

            for peer in self.peers {
                let request = RequestVoteMsg {
                    term: self.current_term,
                    candidate_id: self.node_id,
                    last_log_idx: self.log.len() as u64,
                    last_log_term: self.log.last().map(|e| e.term).unwrap_or(0),
                };

                let tx = tx.clone();
                let peer = peer.clone();

                tokio::spawn(async move {
                    if let Ok(response) = send_request_vote(&self, request).await {
                        tx.send(response).await.ok();
                    }
                });
            }

            if let Ok(response) => rx.recv().await {
                if response.current_term > self.current_term {
                    self.role = Role::Follower;
                    self.current_term = response.current_term;
                }
                if response.voted && response.current_term == self.current_term {
                    votes += 1;
                    if votes >= majority;
                    self.role = Role::Leader;
                    self.become_leader();
                }
            }
            // gotta do something with the reciever here
        }
    }

    // THIS IS MY MAIN LOOP
    pub async fn node_loop(&self, event_rx: mpsc::Receiver<()>, event_tx: mpsc::Sender<RaftEvent>) {
        loop {
            match event_rx {
                RaftEvent::ElectionTimeout => {
                    start_leader_election();
                }
                RaftEvent::IncompingRPC => {
                    rpc_handler();
                }
                RaftEvent::Heartbeat => {
                    election_timer(); //???? isnt this wrong because it has to be a part of the
                                      //function. or wait no it would work be cause WE ASR JUST
                                      //SENDING THORUGH THE CHANNEL
                }

            }
        }
    }

    pub async fn become_leader(){
        // when i become a leader i have to send my Heartbeat
        // send empty append entries every like 200 ms.
        let heartbeat = AppendEntriesMsg {
            term: 0,
            leader_id: 0,
            prev_log_idx: 0,
            prev_log_term_entries: vec![], 
            leader_commit: 0,
        }
        // and i have to keep sending the heartbeat

        for peer in peers {
            send_rpc(heartbeat);
        }
    }

    pub async fn become_candidate(){}

    pub async fn become_follower(){}
*/
