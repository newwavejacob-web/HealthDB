// this is basically our rule book, they sshow thsi shti ib the algorithm
// NOdeState, Role
use crate::raft::messages::LogEntry;
use crate::raft::persist::{self, Meta, Snapshot};
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
              
    // entries after the snapshot: log[0] is index snapshot_index + 1.
    // use the index helpers below instead of indexing log directly.
    pub log: Vec<LogEntry>,
    pub snapshot_index: u64,
    pub snapshot_term: u64,
    // compact once this many applied entries have built up past the snapshot
    pub snapshot_threshold: u64,

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

    // set when a valid leader contacts us or we grant a vote; main resets the election timer
    pub heard_from_leader: bool,

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
            snapshot_index: 0,
            snapshot_term: 0,
            snapshot_threshold: DEFAULT_SNAPSHOT_THRESHOLD,
            commit_index: 0,
            last_applied: 0,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            node_id,
            address,
            peers,
            heard_from_leader: false,
            data_dir,
            persisted: Meta::default(),
        }
    }

    // crash recovery: rebuild term, vote, snapshot, raft log and commit index from disk.
    // returns the snapshot's store contents; the caller loads them into the store and
    // then replays the committed entries after the snapshot.
    pub fn load(node_id: u64, address: String, peers: Vec<String>, data_dir: PathBuf) -> io::Result<(NodeState, HashMap<String, String>)> {
        let mut state = NodeState::new(node_id, address, peers, data_dir);
        let meta = persist::read_meta(&state.meta_path())?;
        let snapshot = persist::read_snapshot(&state.snapshot_path())?;
        state.snapshot_index = snapshot.last_index;
        state.snapshot_term = snapshot.last_term;
        state.log = persist::read_wal(&state.wal_path(), snapshot.last_index)?;
        state.current_term = meta.current_term;
        state.voted_for = meta.voted_for;
        // a snapshot only ever holds committed entries, and we can't have committed
        // more than survived in the WAL
        state.commit_index = meta.commit_index.clamp(state.snapshot_index, state.last_index());
        state.last_applied = state.snapshot_index;
        state.persisted = meta;
        Ok((state, snapshot.data))
    }

    pub fn last_index(&self) -> u64 {
        self.snapshot_index + self.log.len() as u64
    }

    pub fn last_term(&self) -> u64 {
        self.log.last().map(|e| e.term).unwrap_or(self.snapshot_term)
    }

    // term of the entry at `index`, or None if it's past the end or compacted away
    pub fn term_at(&self, index: u64) -> Option<u64> {
        if index == self.snapshot_index {
            Some(self.snapshot_term)
        } else if index > self.snapshot_index && index <= self.last_index() {
            Some(self.log[(index - self.snapshot_index - 1) as usize].term)
        } else {
            None
        }
    }

    // entry at `index`; it must be after the snapshot and not past the end
    pub fn entry(&self, index: u64) -> &LogEntry {
        &self.log[(index - self.snapshot_index - 1) as usize]
    }

    // every entry from `index` on; `index` must be after the snapshot
    pub fn entries_from(&self, index: u64) -> Vec<LogEntry> {
        self.log[(index - self.snapshot_index - 1) as usize..].to_vec()
    }

    pub fn snapshot_path(&self) -> PathBuf {
        persist::snapshot_path(&self.data_dir, self.node_id)
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
        persist::append_wal(&self.wal_path(), self.last_index() + 1, entries).expect("fatal: could not append to WAL");
        self.log.extend_from_slice(entries);
    }

    // drops the entry at `index` and everything after it, in memory and on disk
    pub fn truncate_from(&mut self, index: u64) {
        if index > self.last_index() { return; }
        self.log.truncate((index - self.snapshot_index - 1) as usize);
        persist::rewrite_wal(&self.wal_path(), self.snapshot_index + 1, &self.log).expect("fatal: could not rewrite WAL");
    }

    // log compaction: `data` is the store with everything up to last_applied applied.
    // the snapshot is written first, then the WAL is rewritten without the entries it
    // covers. a crash in between is fine because read_wal skips entries the snapshot covers.
    pub fn compact(&mut self, data: HashMap<String, String>) {
        let last_index = self.last_applied;
        let last_term = self.term_at(last_index).expect("compacting past the end of the log");
        let snapshot = Snapshot { last_index, last_term, data };
        persist::write_snapshot(&self.snapshot_path(), &snapshot).expect("fatal: could not write snapshot");

        self.log.drain(..(last_index - self.snapshot_index) as usize);
        self.snapshot_index = last_index;
        self.snapshot_term = last_term;
        persist::rewrite_wal(&self.wal_path(), last_index + 1, &self.log).expect("fatal: could not rewrite WAL");
        println!("Node {} compacted log through index {} ({} entries left)", self.node_id, last_index, self.log.len());
    }

    // a snapshot from the leader replaces everything it covers (Raft fig. 13 steps 5-7).
    // if our log already has the snapshot's last entry, the entries after it are kept.
    // the caller must load snapshot.data into the store.
    pub fn install_snapshot(&mut self, snapshot: &Snapshot) {
        persist::write_snapshot(&self.snapshot_path(), snapshot).expect("fatal: could not write snapshot");

        if self.term_at(snapshot.last_index) == Some(snapshot.last_term) && snapshot.last_index > self.snapshot_index {
            self.log.drain(..(snapshot.last_index - self.snapshot_index) as usize);
        } else {
            self.log.clear();
        }
        self.snapshot_index = snapshot.last_index;
        self.snapshot_term = snapshot.last_term;
        persist::rewrite_wal(&self.wal_path(), self.snapshot_index + 1, &self.log).expect("fatal: could not rewrite WAL");

        self.commit_index = self.commit_index.max(snapshot.last_index);
        self.last_applied = snapshot.last_index;
        println!("Node {} installed snapshot through index {}", self.node_id, snapshot.last_index);
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

pub const DEFAULT_SNAPSHOT_THRESHOLD: u64 = 1000;

// a fresh, empty data dir per test so tests don't write into the repo or into each other
#[cfg(test)]
pub fn test_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("ddsg-test-{}-{}", std::process::id(), name));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}
