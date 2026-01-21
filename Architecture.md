# HealthDB Architecture

This document provides a deep dive into HealthDB's architecture, design decisions, and implementation details.

---

## System Overview

HealthDB is a distributed key-value store that provides strong consistency guarantees through the Raft consensus algorithm. The system is designed to tolerate node failures while maintaining data integrity.

```
                              ┌──────────────────────────────────────┐
                              │           Client Application         │
                              └──────────────────┬───────────────────┘
                                                 │
                                                 ▼ TCP :6379
┌─────────────────────────────────────────────────────────────────────────────┐
│                                  HealthDB Cluster                           │
│                                                                             │
│  ┌──────────────────────┐  ┌──────────────────────┐  ┌────────────────────┐ │
│  │      Node 1          │  │      Node 2          │  │      Node 3        │ │
│  │    ┌──────────┐      │  │    ┌──────────┐      │  │   ┌──────────┐     │ │
│  │    │ clients  │      │  │    │ clients  │      │  │   │ clients  │     │ │
│  │    └────┬─────┘      │  │    └────┬─────┘      │  │   └────┬─────┘     │ │
│  │         │            │  │         │            │  │        │           │ │
│  │    ┌────▼─────┐      │  │    ┌────▼─────┐      │  │   ┌────▼─────┐     │ │
│  │    │  store   │      │  │    │  store   │      │  │   │  store   │     │ │
│  │    │ HashMap  │      │  │    │ HashMap  │      │  │   │ HashMap  │     │ │
│  │    └────┬─────┘      │  │    └────┬─────┘      │  │   └────┬─────┘     │ │
│  │         │            │  │         │            │  │        │           │ │
│  │    ┌────▼─────┐      │  │    ┌────▼─────┐      │  │   ┌────▼─────┐     │ │
│  │    │   logs   │      │  │    │   logs   │      │  │   │   logs   │     │ │
│  │    │   WAL    │      │  │    │   WAL    │      │  │   │   WAL    │     │ │
│  │    └────┬─────┘      │  │    └────┬─────┘      │  │   └────┬─────┘     │ │
│  │         │            │  │         │            │  │        │           │ │
│  │    ┌────▼─────┐      │  │    ┌────▼─────┐      │  │   ┌────▼─────┐     │ │
│  │    │   raft   │◄─────┼──┼───►│   raft   │◄─────┼──┼──►│   raft   │     │ │
│  │    │ consensus│      │  │    │ consensus│      │  │   │ consensus│     │ │
│  │    └──────────┘      │  │    └──────────┘      │  │   └──────────┘     │ │
│  │                      │  │                      │  │                    │ │
│  │   Role: LEADER       │  │   Role: FOLLOWER     │  │  Role: FOLLOWER    │ │
│  └──────────────────────┘  └──────────────────────┘  └────────────────────┘ │
│               ▲                       ▲                      ▲              │
│               └───────────────────────┴──────────────────────┘              │
│                          Raft RPC (TCP, bincode)                            │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Core Components

### 1. Storage Engine (`store.rs`)

The storage engine is intentionally simple—a thread-safe HashMap:

```rust
pub type Database = Arc<Mutex<HashMap<String, String>>>;
```

**Design Rationale:**
- `Arc` (Atomic Reference Counting) allows multiple threads to own the database
- `Mutex` ensures exclusive access during reads/writes
- Simple to reason about and extend

**Operations:**

| Function | Description |
|----------|-------------|
| `new()` | Creates a new empty database |
| `set(db, key, value, log_flag)` | Inserts/updates a key-value pair |
| `get(db, key)` | Retrieves a value by key |
| `delete(db, key, log_flag)` | Removes a key-value pair |

The `log_flag` parameter controls whether the operation is written to the WAL. This is `false` during WAL replay to prevent duplicate logging.

### 2. Write-Ahead Log (`logs.rs`)

The WAL provides durability—data survives crashes.

**Log Format:**
```
<byte_size> <OPERATION> <key> [value] <crc32_checksum>
```

Examples:
```
15 SET user alice 2847193847
8 DEL user 3948571234
```

**Key Functions:**

| Function | Description |
|----------|-------------|
| `create_log(db)` | Initializes or reloads the log file |
| `log_set(key, value, term)` | Appends a SET operation |
| `log_del(key, term)` | Appends a DEL operation |
| `reload(log_file, db)` | Replays log entries into the database |
| `parse_log(file, db)` | Parses and validates log entries |

**Checksum Validation:**

Each log entry includes a CRC32 checksum computed over the operation payload. During reload, checksums are validated to detect corruption:

```rust
let mut hasher = Hasher::new();
hasher.update(payload);
let checksum = hasher.finalize();

if checksum != write_check {
    eprintln!("Checksum doesn't match, Recheck Data for tampering.");
    continue;
}
```

### 3. Raft Consensus (`raft/`)

The Raft module is the heart of HealthDB's distributed capabilities.

#### State Management (`state.rs`)

```rust
pub enum Role {
    Leader,
    Follower,
    Candidate,
}

pub struct NodeState {
    // Persistent state (survives restarts)
    pub role: Role,
    pub current_term: u64,
    pub voted_for: Option<u64>,
    pub log: Vec<LogEntry>,
    
    // Volatile state
    pub commit_index: u64,
    pub last_applied: u64,
    
    // Leader state (reinitialized after election)
    pub next_index: HashMap<String, u64>,
    pub match_index: HashMap<String, u64>,
    
    // Node identity
    pub node_id: u64,
    pub address: String,
    pub peers: Vec<String>,
}
```

#### Message Types (`messages.rs`)

**RequestVote RPC** - Used during leader election:

```rust
pub struct RequestVoteMsg {
    pub term: u64,           // Candidate's term
    pub candidate_id: u64,   // Candidate requesting vote
    pub last_log_idx: u64,   // Index of candidate's last log entry
    pub last_log_term: u64,  // Term of candidate's last log entry
}

pub struct RequestVoteResponse {
    pub current_term: u64,   // For candidate to update itself
    pub voted: bool,         // True means candidate received vote
}
```

**AppendEntries RPC** - Used for log replication and heartbeats:

```rust
pub struct AppendEntriesMsg {
    pub term: u64,           // Leader's term
    pub leader_id: u64,      // So followers can redirect clients
    pub prev_log_idx: u64,   // Index of log entry preceding new ones
    pub prev_log_term: u64,  // Term of prev_log_idx entry
    pub entries: Vec<LogEntry>,  // Log entries to store (empty for heartbeat)
    pub leader_commit: u64,  // Leader's commit index
}

pub struct AppendEntriesResponse {
    pub current_term: u64,   // For leader to update itself
    pub success: bool,       // True if follower contained matching entry
    pub next_index: u64,     // Hint for faster log catchup
}
```

#### Leader Election (`election.rs`)

The election process follows the Raft specification:

1. **Timeout**: When a follower doesn't receive a heartbeat within the election timeout, it becomes a candidate
2. **Increment Term**: The candidate increments its term and votes for itself
3. **Request Votes**: Sends `RequestVote` RPCs to all peers
4. **Win Condition**: Receives votes from a majority of nodes

```rust
pub async fn start_leader_election(state: &mut NodeState) {
    state.current_term += 1;
    state.voted_for = Some(state.node_id.clone());
    
    let mut votes = 1;  // Vote for self
    let majority = (state.peers.len() + 1) / 2 + 1;
    
    for peer in state.peers.clone() {
        let request = RequestVote(RequestVoteMsg {
            term: state.current_term,
            candidate_id: state.node_id,
            last_log_idx: state.log.len() as u64,
            last_log_term: state.log.last().map(|e| e.term).unwrap_or(0),
        });
        
        match send_rpc(&peer, request).await {
            Ok(RaftMsg::RequestVoteResponse(response)) => {
                if response.voted && response.current_term == state.current_term {
                    votes += 1;
                    if votes >= majority {
                        state.role = Role::Leader;
                    }
                }
            }
            // Handle errors...
        }
    }
}
```

#### Log Replication (`replication.rs`)

Once elected, the leader replicates its log to followers:

1. **Append to Leader Log**: New entries are first appended to the leader's log
2. **Replicate**: Leader sends `AppendEntries` to all followers
3. **Commit**: Once a majority acknowledge, the entry is committed
4. **Apply**: Committed entries are applied to the state machine

```rust
pub async fn log_replication(state: &mut NodeState, log_append: Vec<LogEntry>) {
    for peer in state.peers {
        let next_idx = *state.next_index.get(&peer).unwrap_or(&1);
        let prev_idx = next_idx - 1;
        
        let msg = RaftMsg::AppendEntries(AppendEntriesMsg {
            term: state.current_term,
            leader_id: state.node_id,
            prev_log_idx: prev_idx,
            prev_log_term: /* ... */,
            entries: log_append,
            leader_commit: state.commit_index,
        });
        
        match send_rpc(&peer, msg).await {
            Ok(RaftMsg::AppendEntriesResponse(response)) => {
                if response.success {
                    write_to_logs(state, peer, state.log.len() as u64);
                } else {
                    // Decrement next_index and retry
                    state.next_index.insert(peer, response.next_index);
                }
            }
            // Handle errors...
        }
    }
}
```

#### RPC Layer (`rpc.rs`)

The RPC layer uses TCP with length-prefixed framing and `bincode` serialization:

```rust
pub async fn send_rpc(addr: &str, msg: RaftMsg) -> Result<RaftMsg, Box<dyn Error>> {
    let mut stream = TcpStream::connect(addr).await?;
    
    // Serialize message
    let bytes = bincode::serialize(&msg)?;
    
    // Length-prefixed framing
    let len = bytes.len() as u32;
    stream.write_all(&len.to_be_bytes()).await?;
    stream.write_all(&bytes).await?;
    
    // Read response
    read_rpc(&mut stream).await
}
```

**Wire Format:**
```
┌──────────────┬─────────────────────────────┐
│ Length (4B)  │ bincode-serialized payload  │
│ Big Endian   │                             │
└──────────────┴─────────────────────────────┘
```

---

## Event Loop

The main event loop uses `tokio::select!` to handle multiple async operations:

```rust
loop {
    tokio::select! {
        // Election timeout - start election if no heartbeat received
        _ = sleep(election_timeout) => {
            state.role = Role::Candidate;
            start_leader_election(&mut state).await;
        }
        
        // Heartbeat timeout - leader sends heartbeats
        _ = sleep(Duration::from_millis(125)), if state.role == Role::Leader => {
            send_heartbeats(&state, &peers).await;
        }
        
        // Incoming RPC - handle messages from other nodes
        result = listener.accept() => {
            let (mut stream, _) = result.unwrap();
            if let Ok(msg) = read_rpc(&mut stream).await {
                let response = handle_messages(&mut state, msg);
                write_rpc(&mut stream, response).await;
            }
        }
    }
    
    // Apply committed entries
    if state.commit_index > state.last_applied {
        state.last_applied += 1;
        // Apply to state machine...
    }
}
```

---

## Data Flow

### Write Path

```
1. Client sends: SET key value
          │
          ▼
2. Leader appends to log
          │
          ▼
3. Leader sends AppendEntries to followers
          │
          ▼
4. Followers append and acknowledge
          │
          ▼
5. Leader commits (majority acknowledged)
          │
          ▼
6. Leader applies to HashMap
          │
          ▼
7. Leader returns OK to client
```

### Read Path

```
1. Client sends: GET key
          │
          ▼
2. Read from local HashMap
          │
          ▼
3. Return value to client
```

Note: For linearizable reads, the leader should verify it's still leader before responding. This is a planned enhancement.

---

## Failure Handling

### Leader Failure

1. Followers detect missing heartbeats via election timeout
2. One or more followers become candidates
3. Election proceeds; new leader emerges
4. New leader initializes `next_index` and `match_index` for all peers
5. Log replication resumes from where it left off

### Follower Failure

1. Leader's `AppendEntries` to that follower fails
2. Leader retries on subsequent heartbeats
3. When follower recovers, it catches up via log replication

### Network Partition

1. Minority partition cannot elect a leader (no majority)
2. Majority partition continues operating
3. When partition heals, minority nodes catch up

---

## Configuration

### Port Allocation

| Purpose | Port Pattern |
|---------|--------------|
| Raft RPC | 5000 + node_id |
| Client connections | 6379 (Redis-compatible) |

### Timeouts

| Timeout | Value | Purpose |
|---------|-------|---------|
| Election timeout | 150ms + (node_id * 50ms) | Staggered to reduce split votes |
| Heartbeat interval | 125ms | Leader health signal |

---

## Dependencies

```toml
[dependencies]
tokio = { version = "1", features = ["full"] }
bincode = "1.3"
serde = { version = "1.0", features = ["derive"] }
crc32fast = "1.3"
```

| Crate | Purpose |
|-------|---------|
| `tokio` | Async runtime for concurrent operations |
| `bincode` | Binary serialization for RPC messages |
| `serde` | Serialization framework |
| `crc32fast` | Fast CRC32 checksums for WAL |

---

## Testing Strategy

### Unit Tests

Test individual message handlers:

```rust
#[test]
fn test_append_entries_valid() {
    let mut state = NodeState::new(1, "127.0.0.1:5001".into(), vec![]);
    state.current_term = 1;
    
    let req = AppendEntriesMsg {
        term: 1,
        leader_id: 2,
        prev_log_idx: 0,
        prev_log_term: 0,
        entries: vec![LogEntry { term: 1, data: b"SET foo bar".to_vec() }],
        leader_commit: 0,
    };
    
    let resp = handle_append_entries(&mut state, req);
    assert!(resp.success);
    assert_eq!(state.log.len(), 1);
}
```

### Integration Tests

Spin up multiple nodes and verify:
- Leader election completes
- Writes replicate to followers
- System recovers from node failures

---

## Future Improvements

1. **Persistent State**: Store `currentTerm` and `votedFor` to disk
2. **Log Compaction**: Implement snapshotting to bound log growth
3. **Membership Changes**: Support adding/removing nodes
4. **Linearizable Reads**: Verify leadership before serving reads
5. **Client Request Forwarding**: Redirect clients to leader
6. **Pre-vote Protocol**: Reduce disruption from partitioned nodes
