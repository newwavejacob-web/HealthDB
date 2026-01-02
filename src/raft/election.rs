//vote handliong, elecition logic

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

            for peer in peers {
                let resp = send_rpc(peer, RequestVote{...}).await;
                if resp.voted { votes + = 1; }
            }
            if votes > majority { Become_leader(); }

            loop { sleep(100ms)a.wait;
            fo:}
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

    pub async fn become_leader(){}

    pub async fn become_candidate(){}

    pub async fn become_follower(){}
