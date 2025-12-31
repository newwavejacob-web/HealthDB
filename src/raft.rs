// this is wherer im gonna start building raft shit is gonna be lit'
use std::net::{TcpStream, TcpListener};
use std::fs::File;
use std::io::{Read, Write};
use serde::{Serialize, Deserialize};
use tokio::time::{Duration, sleep};
use tokio::sync::mpsc;

/* raft mesaging */

enum Role {
    Leader,
    Follower,
    Candidate,
}

// our in house per node storage
struct NodeState {
    role: Role,
    current_term: u64,
    voted_for: Option<u64>,
              
    log: Vec<u64>, // is this even waht we want it as or do we want it as a vec of structs
         
    //volState
    commit_index: u64,
    last_applied: u64,

    //leader VolState
    next_index: u64,
    match_index: u64,
    
    node_id: String,
    peers: Vec<String>,
}


#[derive(Serialize, Deserialize, Debug)]
struct AppendEntriesMsg {
    term: u64,
    leader_id: u64,
    prev_log_idx: u64,
    prev_log_term_entries: Vec<u64>, 
    leader_commit: u64,
}

#[derive(Serialize, Deserialize, Debug)]
struct RequestVoteMsg {
    term: u64,
    candidate_id: u64,
    last_log_idx: u64, 
    last_log_term: u64,
}

#[derive(Serialize, Deserialize, Debug)]
struct RequestVoteResponse {
    votedFor: Option<u64>,
    currentTerm: u64,
    voted: bool,
}

#[derive(Serialize, Deserialize)]
struct AppendEntriesResponse {
    currentTerm: u64,
    success: bool,
}

#[derive(Serialize, Deserialize)]
enum RaftMsg {
    AppendEntries(AppendEntriesMsg),
    RequestVote(RequestVoteMsg),
    AppendEntriesResponse(AppendEntriesResponse),
    RequestVoteResponse(RequestVoteResponse),
}

enum RaftEvent {
    IncomingRpc(RaftMsg),
    ElectionTimeout,
    Heartbeat, // this will be append_entries filled with zeroes.
}

impl NodeState {
    //TODO urgent 
    
    //deserialize incoming bytes (into rpc structs)
    //do raft shit
    //serialize outcoming byte

    /* --- COMMUNICATION LAYER HERE --- */

    //USE SERDE

    // send rpc
    pub fn send_node_msg (&self, node_id: &str, msg_type: RaftMsg,) {
        let mut stream = TcpStream::connect(node_id).unwrap();
        // write info to stream here, this is our basis. we gonnna be basically just sending serde
        match msg_type {
            // so look here we are literally saying to match on MSG FUCKING TYPE, so we have to
            // fucking match on this type dumbass
            RaftMsg::AppendEntries(ae) =>{
                todo!();
            }
            RaftMsg::AppendEntriesResponse(resp) =>{
                todo!();
            }
            RaftMsg::RequestVote(rv) => {
                todo!();
            }
            RaftMsg::RequestVoteResponse(resp) => {
                todo!();
            }
            _ => eprintln!("not valid raft RPC")   
        }
    }
    pub fn recieve_node_msg (&self, node_id: &str) {
        let listener = TcpListener::bind(node_id).unwrap();
        
        for stream in listener.incoming() {
            match stream {

                Ok(stream) => {
                    let mut buffer = [0u8; 1024];
                    let read = stream.read(&mut buffer).unwrap(); 
                    let msg: RaftMsg = bincode::deserialize(&buffer[..read]).unwrap();

                        // DO DIFFERENT LOGIC DEPENDING ON WHAT WE REVIECE.
                        // CALL FUNCTIONS here
                        match msg {
                            RaftMsg::AppendEntries(ae) => {
                                todo!();
                            }
                            RaftMsg::RequestVote(rv) => {
                                todo!();
                            }
                            RaftMsg::AppendEntriesResponse(resp) => {
                                todo!();
                            }
                            RaftMsg::RequestVoteResponse(resp) => {
                                todo!();
                            }
                        }
                }

                Err(e) => eprintln!("Connection failed: {}", e),
            }
        }
    }

    //ik  what its supposed to look like 
    //serde everything into nodes.
    //pass nodes everywhere
    //if node is leader, then send heartbeat every like fucking 200 ms idk.
    //
    //I HAVE TO SEND EVERYTHING TO EVERYTHING, WE NEED MORE CONCURRENCY NOT JUST COMMUNCATION

    // THIS IS MY tCP LISteNER TASK
    //TODO have to make this my listener for RPC
    pub async fn rpc_handler(&self, event_tx: mpsc::Sender<RaftEvent>) {
        let listener = TcpListener::bind(...);
        loop {
            let stream = listener.accept();
            let msg = deserialize_from(stream);

            event_tx.send(RaftEvent::IncomingRpc(msg)).await;
        }
    }

    // THIS IS MY TIMER TASK
    // this is our heartbeat
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
 /*    pub fn heartbeat(&self, node_id: &str){
        let mut stream = TcpStream::connect(node_id).unwrap();

        let heartbeat_msg = AppendEntriesMsg {
            term,
            leader_id,
            prev_log_idx: 0,
            prev_log_term_entries: vec![],
            leader_commit: 0,
        };
        let stream_write = bincode::serialize(&heartbeat_msg).unwrap();

        let mut buffer = [0u8; 1024];
        let read = stream.read(&mut buffer).unwrap();
        println!("revieved H34RTB33T {:?} ", &buffer[..read]);
        
        stream.write_all(stream_write);
    }
*/
    // TODO 
    pub async fn start_election(node: Node, node_state: Nodestate){
        Node::self.current_term += 1;
        Node::self.voted_for = some(self.node_id);
        let mut votes = 1;

        for node in nodes {
            if vote_success && response.term = self.current_term {
                votes += 1;

                if votes > (num_nodes / 2) {
                    let Node::role = leader;
                }
            }
        }
    }

    pub async fn state_rules(&self, mpsc::Sender<RaftEvent>, mpsc::Reciever<RaftEvent>){
        if commit_index > last_applied {
            last_applied += 1;
            logs::append_log(log[last_applied]);
        }

        match self.state {
            node::follower => {

            }
            node::candidate => {
                for node in nodes {
                    start_election(node,node_state);
                }
                //start election, send request_vode rpc
                //count number of responses, based off our up to date and rpc rules,
                //thats our vote, if larger than cluster size, we become leader,
                //if we see a higher term, become follower
            }
            node::leader => {
                if term >= current_term {
                    Node::become_follower;
                }
                send_heartbeat();
//                log_replication();
                // send empty Append Entries rpc to keep followers alive
                // send not empty Append entries for log replication, thats the fun part.
                // if it sees higher term, becomes follower, new leader election
            }
        }
    }
}

    /*
    // TODO

    // LOG REPLICATION
    //invoked by leader


    // we arent actually voting here, we are checking logs to get a "vote"
    // i will be doing this after leader election
    //
    // this is gonna be reliant off our async logs, and using our leader. 
    /*pub fn log_replication () {
        if AppendEntriesResponse() = success {
            logs::append();
        }
        // here we have to do the condition where i make so that if the replication has the same index
        // and different term, we delete
        else if idx != term {}
        else {
            todo!("appending logic here for error");
        }
    }
}*/
