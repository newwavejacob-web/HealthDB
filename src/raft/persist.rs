// everything raft keeps on disk, per node, inside its data dir:
//   log-<id>.txt   the WAL. one entry per line: "<term> <len> <CMD> <args...> <crc32>\n"
//   meta-<id>.txt  "<current_term> <voted_for or -> <commit_index>\n"
// every write is fsynced before it returns, so anything the node has acted on
// (a vote it granted, an entry it acked) survives a crash.
use crate::raft::messages::LogEntry;
use crate::logs;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

pub fn wal_path(dir: &Path, node_id: u64) -> PathBuf {
    dir.join(format!("log-{}.txt", node_id))
}

pub fn meta_path(dir: &Path, node_id: u64) -> PathBuf {
    dir.join(format!("meta-{}.txt", node_id))
}

#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct Meta {
    pub current_term: u64,
    pub voted_for: Option<u64>,
    pub commit_index: u64,
}

fn encode_entry(entry: &LogEntry) -> Vec<u8> {
    let mut line = format!("{} ", entry.term).into_bytes();
    line.extend_from_slice(&entry.data);
    line
}

pub fn append_wal(path: &Path, entries: &[LogEntry]) -> io::Result<()> {
    let mut file = OpenOptions::new().append(true).create(true).open(path)?;
    for entry in entries {
        file.write_all(&encode_entry(entry))?;
    }
    file.sync_data()
}

// replaces the whole WAL. only needed when a follower drops conflicting entries,
// which is rare, so rewriting beats a more complicated on-disk format.
pub fn rewrite_wal(path: &Path, entries: &[LogEntry]) -> io::Result<()> {
    let mut bytes = Vec::new();
    for entry in entries {
        bytes.extend(encode_entry(entry));
    }
    write_atomic(path, &bytes)
}

// reads every intact entry. a crash mid-append can leave a partial or corrupt last line;
// everything from the first bad record on is cut off the file, since the node never
// acknowledged it (acks only go out after the fsync).
pub fn read_wal(path: &Path) -> io::Result<Vec<LogEntry>> {
    let bytes = match fs::read(path) {
        Ok(b) => b,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };

    let mut entries = Vec::new();
    let mut good_len = 0;
    for line in bytes.split_inclusive(|&b| b == b'\n') {
        match decode_entry(line) {
            Some(entry) => {
                entries.push(entry);
                good_len += line.len();
            }
            None => break,
        }
    }

    if good_len < bytes.len() {
        eprintln!("WAL {}: dropping {} bytes of torn/corrupt tail", path.display(), bytes.len() - good_len);
        let file = OpenOptions::new().write(true).open(path)?;
        file.set_len(good_len as u64)?;
        file.sync_all()?;
    }
    Ok(entries)
}

fn decode_entry(line: &[u8]) -> Option<LogEntry> {
    if !line.ends_with(b"\n") {
        return None;
    }
    let text = std::str::from_utf8(line).ok()?;
    let (term, data) = text.split_once(' ')?;
    let term = term.parse().ok()?;
    if !logs::checksum_ok(data) {
        return None;
    }
    Some(LogEntry { term, data: data.as_bytes().to_vec() })
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
    use crate::raft::{NodeState, AppendEntriesMsg, RequestVoteMsg, RaftMsg, handle_messages, test_dir};

    // simulates kill -9: the old NodeState is dropped without any shutdown step,
    // and a new one is built purely from what's on disk
    fn restart(state: NodeState) -> NodeState {
        let dir = state.data_dir.clone();
        drop(state);
        NodeState::load(1, "127.0.0.1:5001".into(), vec![], dir).unwrap()
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
        assert!(voted(handle_messages(&mut state, vote_request(3, 2))));

        let mut state = restart(state);
        assert_eq!(state.current_term, 3);
        assert_eq!(state.voted_for, Some(2));
        // a different candidate in the same term must be refused
        assert!(!voted(handle_messages(&mut state, vote_request(3, 3))));
    }

    #[test]
    fn log_and_commit_survive_restart() {
        let mut state = NodeState::new(1, "127.0.0.1:5001".into(), vec![], test_dir("log-restart"));
        let entries = vec![logs::log_set("a", "1", 1), logs::log_set("b", "two words", 1)];
        handle_messages(&mut state, append(1, 0, 0, entries.clone(), 1));

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
        handle_messages(&mut state, append(1, 0, 0, old, 0));
        // a new leader in term 2 overwrites everything after index 1
        handle_messages(&mut state, append(2, 1, 1, vec![logs::log_set("b", "new", 2)], 0));
        assert_eq!(state.log.len(), 2);

        let state = restart(state);
        assert_eq!(state.log.len(), 2);
        assert_eq!(state.log[1].term, 2);
    }

    #[test]
    fn torn_tail_is_dropped_on_restart() {
        let mut state = NodeState::new(1, "127.0.0.1:5001".into(), vec![], test_dir("torn-tail"));
        handle_messages(&mut state, append(1, 0, 0, vec![logs::log_set("a", "1", 1)], 0));
        let good_len = fs::metadata(state.wal_path()).unwrap().len();
        // crash halfway through writing the next entry
        OpenOptions::new().append(true).open(state.wal_path()).unwrap().write_all(b"1 15 SET b 2 12").unwrap();

        let state = restart(state);
        assert_eq!(state.log.len(), 1);
        assert_eq!(fs::metadata(state.wal_path()).unwrap().len(), good_len);
    }
}
