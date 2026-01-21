# HealthDB

A distributed, fault-tolerant key-value database built in Rust, implementing the Raft consensus algorithm for reliable data replication across nodes.

**Status:** Active Development | **Author:** Jacob | **License:** MIT

---

## What is HealthDB?

HealthDB is a distributed database designed for medical IoT coordination—enabling devices to work together rather than in isolation. At its core, it's a key-value store that maintains consistency across multiple nodes using the Raft consensus protocol.

### Key Features

- **Raft Consensus**: Leader election, log replication, and commit management
- **Write-Ahead Logging (WAL)**: Durability with CRC32 checksums for corruption detection
- **Concurrent Access**: Thread-safe operations using `Arc<Mutex<>>` patterns
- **TCP-based RPC**: Custom binary protocol for inter-node communication
- **Crash Recovery**: Automatic state restoration from WAL on restart

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                         HealthDB Cluster                            │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│   ┌─────────────┐     ┌─────────────┐     ┌─────────────┐          │
│   │   Node 1    │     │   Node 2    │     │   Node 3    │          │
│   │  (Leader)   │────▶│ (Follower)  │     │ (Follower)  │          │
│   │             │     │             │◀────│             │          │
│   └─────────────┘     └─────────────┘     └─────────────┘          │
│         │                   ▲                   ▲                   │
│         │                   │                   │                   │
│         └───────────────────┴───────────────────┘                   │
│                    Raft RPC (AppendEntries, RequestVote)            │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### Core Components

| Module | Purpose |
|--------|---------|
| `store.rs` | Core key-value storage engine (`Arc<Mutex<HashMap>>`) |
| `logs.rs` | Write-Ahead Log implementation with checksums |
| `raft/` | Raft consensus module |
| ├─ `state.rs` | Node state management (Role, Term, Log) |
| ├─ `messages.rs` | RPC message types (AppendEntries, RequestVote) |
| ├─ `election.rs` | Leader election logic |
| ├─ `replication.rs` | Log replication and commit advancement |
| └─ `rpc.rs` | TCP-based RPC layer |
| `clients.rs` | Client connection handling and command parsing |
| `server.rs` | TCP listener for client connections |

---

## Quick Start

### Prerequisites

- Rust 1.70+ (`rustup update stable`)
- Tokio runtime (included in dependencies)

### Running a 3-Node Cluster

```bash
# Terminal 1 - Start Node 1
cargo run -- 1

# Terminal 2 - Start Node 2
cargo run -- 2

# Terminal 3 - Start Node 3
cargo run -- 3
```

Nodes automatically discover peers on ports 5001-5003.

### Connecting a Client

```bash
# Connect via netcat
nc localhost 6379

# Commands
SET mykey myvalue
GET mykey
DEL mykey
```

---

## How It Works

### Raft Consensus

HealthDB implements the [Raft consensus algorithm](https://raft.github.io/) for distributed coordination:

1. **Leader Election**: When a leader fails or the cluster starts, nodes run an election. A candidate needs a majority vote to become leader.

2. **Log Replication**: The leader accepts client requests, appends them to its log, and replicates entries to followers via `AppendEntries` RPCs.

3. **Commit & Apply**: Once a majority of nodes have replicated an entry, the leader advances the commit index. All nodes then apply committed entries to their state machines.

### Write-Ahead Logging

Every mutation is logged before being applied:

```
Format: <byte_size> <OP> <key> [value] <crc32_checksum>
Example: 15 SET user1 alice 3847293847
```

On restart, the WAL is replayed to restore state. Checksums detect corruption.

### State Machine

```
Client Request → Leader Log → Replicate to Followers → Commit → Apply to HashMap
```

---

## Project Structure

```
healthdb/
├── src/
│   ├── main.rs          # Entry point, Tokio event loop
│   ├── store.rs         # Key-value storage
│   ├── logs.rs          # Write-Ahead Log
│   ├── server.rs        # TCP server
│   ├── clients.rs       # Client handling
│   └── raft/
│       ├── mod.rs       # Module exports
│       ├── state.rs     # NodeState, Role enum
│       ├── messages.rs  # RPC message types
│       ├── election.rs  # Leader election
│       ├── replication.rs # Log replication
│       └── rpc.rs       # RPC send/receive
├── tests/
│   └── raft_tests.rs    # Integration tests
├── Cargo.toml
└── README.md
```

---

## Technical Decisions

### Why Rust?

- Memory safety without garbage collection—critical for database reliability
- Zero-cost abstractions for high performance
- Excellent async ecosystem (Tokio) for concurrent network operations
- Strong type system catches bugs at compile time

### Why Raft over Paxos?

- Raft was designed for understandability without sacrificing correctness
- Clear separation of concerns (leader election, log replication, safety)
- Easier to implement and debug

### Concurrency Model

```rust
// Thread-safe database handle
pub type Database = Arc<Mutex<HashMap<String, String>>>;

// Async RPC with Tokio
pub async fn send_rpc(addr: &str, msg: RaftMsg) -> Result<RaftMsg, Box<dyn Error>> {
    let mut stream = TcpStream::connect(addr).await?;
    // Binary serialization with bincode
    let bytes = bincode::serialize(&msg)?;
    // Length-prefixed framing
    stream.write_all(&len.to_be_bytes()).await?;
    stream.write_all(&bytes).await?;
    read_rpc(&mut stream).await
}
```

---

## Current Status & Roadmap

### Completed
- [x] Concurrent key-value store with `Arc<Mutex<HashMap>>`
- [x] Write-Ahead Log with CRC32 checksums
- [x] WAL reload on startup
- [x] Raft message types and serialization
- [x] TCP-based RPC layer
- [x] Leader election
- [x] RequestVote and AppendEntries handlers
- [x] Basic log replication

### In Progress
- [ ] Commit index advancement
- [ ] Apply committed entries to state machine
- [ ] Client request forwarding to leader

### Planned
- [ ] Snapshotting for log compaction
- [ ] Membership changes (adding/removing nodes)
- [ ] Persistent term and votedFor
- [ ] Benchmarking and performance optimization

---

## Development

### Building

```bash
cargo build --release
```

### Testing

```bash
cargo test
```

### Running with Debug Output

```bash
RUST_LOG=debug cargo run -- 1
```

---

## Learning Resources

These resources were invaluable during development:

- [The Raft Paper](https://raft.github.io/raft.pdf) - The original Raft paper by Ongaro and Ousterhout
- [Raft Visualization](https://raft.github.io/) - Interactive visualization of the algorithm
- [Tokio Tutorial](https://tokio.rs/tokio/tutorial) - Async Rust with Tokio
- [The Rust Book](https://doc.rust-lang.org/book/) - Rust fundamentals

---

## Author

**Jacob** — CS Senior at UCF, Vice President of CHIDA

Built as a senior design project exploring distributed systems, fault tolerance, and systems programming in Rust.

---

## License

MIT License - See [LICENSE](LICENSE) for details.
