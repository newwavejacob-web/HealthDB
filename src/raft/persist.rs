// everything raft keeps on disk, per node, inside its data dir:
//   log-<id>.txt       the WAL. one entry per line: "<index> <term> <len> <CMD> <args...> <crc32>\n"
//   meta-<id>.txt      "<current_term> <voted_for or -> <commit_index>\n"
//   snapshot-<id>.bin  the store as of some log index (bincode), see Snapshot
// every write is fsynced before it returns, so anything the node has acted on
// (a vote it granted, an entry it acked) survives a crash.
use crate::raft::messages::LogEntry;
use crate::logs;
use serde::{Serialize, Deserialize};
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

pub fn wal_path(dir: &Path, node_id: u64) -> PathBuf {
    dir.join(format!("log-{}.txt", node_id))
}

pub fn meta_path(dir: &Path, node_id: u64) -> PathBuf {
    dir.join(format!("meta-{}.txt", node_id))
}

pub fn snapshot_path(dir: &Path, node_id: u64) -> PathBuf {
    dir.join(format!("snapshot-{}.bin", node_id))
}

// the whole store as of log index last_index. everything up to and including that
// index can be dropped from the log.
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq)]
pub struct Snapshot {
    pub last_index: u64,
    pub last_term: u64,
    pub data: HashMap<String, String>,
}

pub fn write_snapshot(path: &Path, snapshot: &Snapshot) -> io::Result<()> {
    let bytes = bincode::serialize(snapshot).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    write_atomic(path, &bytes)
}

pub fn read_snapshot(path: &Path) -> io::Result<Snapshot> {
    match fs::read(path) {
        Ok(bytes) => bincode::deserialize(&bytes).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e)),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(Snapshot::default()),
        Err(e) => Err(e),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct Meta {
    pub current_term: u64,
    pub voted_for: Option<u64>,
    pub commit_index: u64,
}

// each record carries its own log index, so after compaction the WAL can start anywhere,
// and stale records left by a crash between writing a snapshot and rewriting the WAL
// are recognised and skipped
fn encode_entries(first_index: u64, entries: &[LogEntry]) -> Vec<u8> {
    let mut bytes = Vec::new();
    for (i, entry) in entries.iter().enumerate() {
        bytes.extend(format!("{} {} ", first_index + i as u64, entry.term).into_bytes());
        bytes.extend_from_slice(&entry.data);
    }
    bytes
}

pub fn append_wal(path: &Path, first_index: u64, entries: &[LogEntry]) -> io::Result<()> {
    let mut file = OpenOptions::new().append(true).create(true).open(path)?;
    file.write_all(&encode_entries(first_index, entries))?;
    file.sync_data()
}

// replaces the whole WAL. needed when a follower drops conflicting entries or after
// compaction, both rare enough that rewriting beats a more complicated on-disk format.
pub fn rewrite_wal(path: &Path, first_index: u64, entries: &[LogEntry]) -> io::Result<()> {
    write_atomic(path, &encode_entries(first_index, entries))
}

// returns the entries after `after_index` (the snapshot point), in order.
// a crash mid-append can leave a partial or corrupt last line;
// everything from the first bad record on is cut off the file, since the node never
// acknowledged it (acks only go out after the fsync).
pub fn read_wal(path: &Path, after_index: u64) -> io::Result<Vec<LogEntry>> {
    let bytes = match fs::read(path) {
        Ok(b) => b,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };

    let mut entries = Vec::new();
    let mut good_len = 0;
    for line in bytes.split_inclusive(|&b| b == b'\n') {
        let (index, entry) = match decode_entry(line) {
            Some(decoded) => decoded,
            None => break,
        };
        good_len += line.len();
        if index <= after_index {
            continue; // already covered by the snapshot
        }
        let expected = after_index + entries.len() as u64 + 1;
        if index != expected {
            return Err(io::Error::new(io::ErrorKind::InvalidData,
                format!("WAL {} has entry {} where {} was expected", path.display(), index, expected)));
        }
        entries.push(entry);
    }

    if good_len < bytes.len() {
        eprintln!("WAL {}: dropping {} bytes of torn/corrupt tail", path.display(), bytes.len() - good_len);
        let file = OpenOptions::new().write(true).open(path)?;
        file.set_len(good_len as u64)?;
        file.sync_all()?;
    }
    Ok(entries)
}

fn decode_entry(line: &[u8]) -> Option<(u64, LogEntry)> {
    if !line.ends_with(b"\n") {
        return None;
    }
    let text = std::str::from_utf8(line).ok()?;
    let (index, rest) = text.split_once(' ')?;
    let (term, data) = rest.split_once(' ')?;
    if !logs::checksum_ok(data) {
        return None;
    }
    Some((index.parse().ok()?, LogEntry { term: term.parse().ok()?, data: data.as_bytes().to_vec() }))
}

pub fn write_meta(path: &Path, meta: &Meta) -> io::Result<()> {
    let voted = meta.voted_for.map(|v| v.to_string()).unwrap_or_else(|| "-".to_string());
    let text = format!("{} {} {}\n", meta.current_term, voted, meta.commit_index);
    write_atomic(path, text.as_bytes())
}

pub fn read_meta(path: &Path) -> io::Result<Meta> {
    let text = match fs::read_to_string(path) {
        Ok(t) => t,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Meta::default()),
        Err(e) => return Err(e),
    };
    let bad = || io::Error::new(io::ErrorKind::InvalidData, format!("corrupt meta file {}", path.display()));
    let parts: Vec<&str> = text.split_whitespace().collect();
    if parts.len() != 3 {
        return Err(bad());
    }
    Ok(Meta {
        current_term: parts[0].parse().map_err(|_| bad())?,
        voted_for: if parts[1] == "-" { None } else { Some(parts[1].parse().map_err(|_| bad())?) },
        commit_index: parts[2].parse().map_err(|_| bad())?,
    })
}

// write to a temp file, fsync, rename over the target, fsync the directory.
// a crash at any point leaves either the old file or the new one, never half of each.
pub fn write_atomic(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let tmp = path.with_extension("tmp");
    {
        let mut file = File::create(&tmp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
    }
    fs::rename(&tmp, path)?;
    let dir = path.parent().filter(|p| !p.as_os_str().is_empty()).unwrap_or(Path::new("."));
    File::open(dir)?.sync_all()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::{NodeState, AppendEntriesMsg, RequestVoteMsg, InstallSnapshotMsg, RaftMsg, handle_messages, apply_committed, test_dir};
    use crate::store::{self, Database};

    // simulates kill -9: the old NodeState is dropped without any shutdown step,
    // and a new one is built purely from what's on disk
    fn restart(state: NodeState) -> NodeState {
        let dir = state.data_dir.clone();
        drop(state);
        NodeState::load(1, "127.0.0.1:5001".into(), vec![], dir).unwrap().0
    }

    fn db() -> Database {
        store::new()
    }

    fn vote_request(term: u64, candidate_id: u64) -> RaftMsg {
        RaftMsg::RequestVote(RequestVoteMsg { term, candidate_id, last_log_idx: 0, last_log_term: 0 })
    }

    fn append(term: u64, prev_log_idx: u64, prev_log_term: u64, entries: Vec<LogEntry>, leader_commit: u64) -> RaftMsg {
        RaftMsg::AppendEntries(AppendEntriesMsg { term, leader_id: 2, prev_log_idx, prev_log_term, entries, leader_commit })
    }

    fn voted(msg: RaftMsg) -> bool {
        match msg { RaftMsg::RequestVoteResponse(r) => r.voted, _ => panic!("expected a vote response") }
    }

    #[test]
    fn vote_survives_restart_so_node_cannot_vote_twice() {
        let mut state = NodeState::new(1, "127.0.0.1:5001".into(), vec![], test_dir("vote-restart"));
        assert!(voted(handle_messages(&mut state, vote_request(3, 2), &db())));

        let mut state = restart(state);
        assert_eq!(state.current_term, 3);
        assert_eq!(state.voted_for, Some(2));
        // a different candidate in the same term must be refused
        assert!(!voted(handle_messages(&mut state, vote_request(3, 3), &db())));
    }

    #[test]
    fn log_and_commit_survive_restart() {
        let mut state = NodeState::new(1, "127.0.0.1:5001".into(), vec![], test_dir("log-restart"));
        let entries = vec![logs::log_set("a", "1", 1), logs::log_set("b", "two words", 1)];
        handle_messages(&mut state, append(1, 0, 0, entries.clone(), 1), &db());

        let state = restart(state);
        assert_eq!(state.log.len(), 2);
        assert_eq!(state.log[1].data, entries[1].data);
        assert_eq!(state.commit_index, 1);
        assert_eq!(state.last_applied, 0, "store is rebuilt by replaying committed entries");
    }

    #[test]
    fn conflicting_entries_stay_truncated_after_restart() {
        let mut state = NodeState::new(1, "127.0.0.1:5001".into(), vec![], test_dir("truncate-restart"));
        let old = vec![logs::log_set("a", "1", 1), logs::log_set("b", "2", 1), logs::log_set("c", "3", 1)];
        handle_messages(&mut state, append(1, 0, 0, old, 0), &db());
        // a new leader in term 2 overwrites everything after index 1
        handle_messages(&mut state, append(2, 1, 1, vec![logs::log_set("b", "new", 2)], 0), &db());
        assert_eq!(state.log.len(), 2);

        let state = restart(state);
        assert_eq!(state.log.len(), 2);
        assert_eq!(state.log[1].term, 2);
    }

    #[test]
    fn torn_tail_is_dropped_on_restart() {
        let mut state = NodeState::new(1, "127.0.0.1:5001".into(), vec![], test_dir("torn-tail"));
        handle_messages(&mut state, append(1, 0, 0, vec![logs::log_set("a", "1", 1)], 0), &db());
        let good_len = fs::metadata(state.wal_path()).unwrap().len();
        // crash halfway through writing the next entry
        OpenOptions::new().append(true).open(state.wal_path()).unwrap().write_all(b"2 1 15 SET b 2 12").unwrap();

        let state = restart(state);
        assert_eq!(state.log.len(), 1);
        assert_eq!(fs::metadata(state.wal_path()).unwrap().len(), good_len);
    }

    fn entries(term: u64, keys: &[&str]) -> Vec<LogEntry> {
        keys.iter().map(|k| logs::log_set(k, &format!("{}-value", k), term)).collect()
    }

    #[test]
    fn compaction_then_restart_restores_store_from_snapshot_plus_wal_tail() {
        let mut state = NodeState::new(1, "127.0.0.1:5001".into(), vec![], test_dir("compact-restart"));
        state.snapshot_threshold = 3;
        let store = db();
        handle_messages(&mut state, append(1, 0, 0, entries(1, &["a", "b", "c", "d"]), 4), &store);
        apply_committed(&mut state, &store);
        assert_eq!(state.snapshot_index, 4);
        assert!(state.log.is_empty());

        // two more entries after the snapshot, only the first committed
        handle_messages(&mut state, append(1, 4, 1, entries(1, &["e", "f"]), 5), &store);
        apply_committed(&mut state, &store);

        let (mut state, data) = NodeState::load(1, "127.0.0.1:5001".into(), vec![], state.data_dir.clone()).unwrap();
        assert_eq!((state.snapshot_index, state.last_index(), state.commit_index), (4, 6, 5));
        let store = db();
        *store.lock().unwrap() = data;
        apply_committed(&mut state, &store);
        let map = store.lock().unwrap();
        for k in ["a", "b", "c", "d", "e"] {
            assert_eq!(map.get(k).map(|s| s.as_str()), Some(format!("{}-value", k).as_str()));
        }
        assert!(!map.contains_key("f"), "uncommitted entry must not be applied");
    }

    #[test]
    fn crash_between_snapshot_and_wal_rewrite_is_recoverable() {
        let mut state = NodeState::new(1, "127.0.0.1:5001".into(), vec![], test_dir("compact-crash"));
        handle_messages(&mut state, append(1, 0, 0, entries(1, &["a", "b", "c", "d", "e"]), 5), &db());
        // snapshot through 3 hit the disk, but the WAL still holds entries 1-5
        let snapshot = Snapshot { last_index: 3, last_term: 1, data: HashMap::new() };
        write_snapshot(&state.snapshot_path(), &snapshot).unwrap();

        let state = restart(state);
        assert_eq!(state.snapshot_index, 3);
        assert_eq!(state.log.len(), 2);
        assert_eq!(state.last_index(), 5);
    }

    #[test]
    fn follower_installs_snapshot_and_continues_from_it() {
        let mut state = NodeState::new(1, "127.0.0.1:5001".into(), vec![], test_dir("install"));
        let store = db();
        let mut data = HashMap::new();
        data.insert("a".to_string(), "1".to_string());
        let msg = RaftMsg::InstallSnapshot(InstallSnapshotMsg {
            term: 2, leader_id: 2,
            snapshot: Snapshot { last_index: 10, last_term: 2, data },
        });
        handle_messages(&mut state, msg, &store);
        assert_eq!(store.lock().unwrap().get("a").map(|s| s.as_str()), Some("1"));
        assert_eq!((state.snapshot_index, state.commit_index, state.last_applied), (10, 10, 10));

        // an AppendEntries that starts before the snapshot only adds what's new
        match handle_messages(&mut state, append(2, 8, 2, entries(2, &["x", "y", "z"]), 11), &store) {
            RaftMsg::AppendEntriesResponse(r) => assert!(r.success),
            _ => panic!(),
        }
        assert_eq!(state.last_index(), 11);

        // and it all survives a restart
        let state = restart(state);
        assert_eq!((state.snapshot_index, state.last_index()), (10, 11));
    }

    #[test]
    fn conflicting_term_hint_skips_the_whole_term() {
        let mut state = NodeState::new(1, "127.0.0.1:5001".into(), vec![], test_dir("conflict-hint"));
        let mut log = entries(1, &["a", "b"]);
        log.extend(entries(2, &["c", "d"]));
        handle_messages(&mut state, append(2, 0, 0, log, 0), &db());

        // leader's entry 4 is from term 3, ours is from term 2: back up to the start of term 2
        match handle_messages(&mut state, append(3, 4, 3, vec![], 0), &db()) {
            RaftMsg::AppendEntriesResponse(r) => assert_eq!((r.success, r.next_index), (false, 3)),
            _ => panic!(),
        }
    }
}
