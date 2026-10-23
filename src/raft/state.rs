// this is basically our rule book, they sshow thsi shti ib the algorithm
// NOdeState, Role
use crate::raft::messages::LogEntry;
use crate::raft::persist::{self, Meta};
use std::collections::HashMap;
use std::io;
use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq)]
pub enum Role {
    Leader,
    Follower,
    Candidate,
}

// our in house per node storage
pub struct NodeState {
    pub role: Role,
    pub current_term: u64,
    pub voted_for: Option<u64>,
              
    pub log: Vec<LogEntry>, // is this even waht we want it as or do we want it as a vec of structs
         
    //volState
    pub commit_index: u64,
    pub last_applied: u64,

    //leader VolState
    pub next_index: HashMap<String, u64>,
    pub match_index: HashMap<String, u64>,
    
    pub node_id: u64,
    #[allow(dead_code)] // this node's own bind address; kept in state, not read yet
    pub address: String,
    pub peers: Vec<String>,

    // where the WAL and meta file live, and what the meta file currently says
    pub data_dir: PathBuf,
    persisted: Meta,
}

impl NodeState {
    pub fn new(node_id: u64, address: String, peers: Vec<String>, data_dir: PathBuf) -> NodeState {
        NodeState {
            role: Role::Follower,
            current_term: 0,
            voted_for: None,
            log: Vec::new(),
            commit_index: 0,
            last_applied: 0,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            node_id,
            address,
            peers,
            data_dir,
            persisted: Meta::default(),
        }
    }

    // crash recovery: rebuild term, vote, raft log and commit index from disk.
    // last_applied starts at 0 so the caller replays committed entries into the store.
    pub fn load(node_id: u64, address: String, peers: Vec<String>, data_dir: PathBuf) -> io::Result<NodeState> {
        let mut state = NodeState::new(node_id, address, peers, data_dir);
        let meta = persist::read_meta(&state.meta_path())?;
        state.log = persist::read_wal(&state.wal_path())?;
        state.current_term = meta.current_term;
        state.voted_for = meta.voted_for;
        // can't have committed more than survived in the WAL
        state.commit_index = meta.commit_index.min(state.log.len() as u64);
        state.persisted = meta;
        Ok(state)
    }

    pub fn wal_path(&self) -> PathBuf {
        persist::wal_path(&self.data_dir, self.node_id)
    }

    pub fn meta_path(&self) -> PathBuf {
        persist::meta_path(&self.data_dir, self.node_id)
    }

    // appends to the WAL (fsynced) and then to the in-memory log.
    // a node that can't write its log can't safely take part in raft, so failure is fatal.
    pub fn append_entries(&mut self, entries: &[LogEntry]) {
        if entries.is_empty() { return; }
        persist::append_wal(&self.wal_path(), entries).expect("fatal: could not append to WAL");
        self.log.extend_from_slice(entries);
    }

    // drops every entry from position `len` on, in memory and on disk
    pub fn truncate_log(&mut self, len: usize) {
        if len >= self.log.len() { return; }
        self.log.truncate(len);
        persist::rewrite_wal(&self.wal_path(), &self.log).expect("fatal: could not rewrite WAL");
    }

    // writes term/vote/commit to disk if they changed. call this before replying to any
    // RPC or sending vote requests, so a vote or term the node has acted on can't be forgotten.
    pub fn persist_meta(&mut self) {
        let meta = Meta {
            current_term: self.current_term,
            voted_for: self.voted_for,
            commit_index: self.commit_index,
        };
        if meta == self.persisted { return; }
        persist::write_meta(&self.meta_path(), &meta).expect("fatal: could not persist raft metadata");
        self.persisted = meta;
    }
}

// a fresh, empty data dir per test so tests don't write into the repo or into each other
#[cfg(test)]
pub fn test_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("ddsg-test-{}-{}", std::process::id(), name));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}
