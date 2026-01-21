# Getting Started with HealthDB

This guide will help you get HealthDB running locally and understand how to interact with the cluster.

---

## Prerequisites

### Required Software

- **Rust 1.70+** 
  ```bash
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
  rustup update stable
  ```

- **Git**
  ```bash
  sudo apt install git  # Ubuntu/Debian
  brew install git      # macOS
  ```

### Verify Installation

```bash
rustc --version   # Should show 1.70.0 or higher
cargo --version   # Should show 1.70.0 or higher
```

---

## Quick Start

### 1. Clone the Repository

```bash
git clone https://github.com/yourusername/healthdb.git
cd healthdb
```

### 2. Build the Project

```bash
cargo build --release
```

This compiles the project and places the binary in `target/release/`.

### 3. Start a 3-Node Cluster

Open three terminal windows:

**Terminal 1 - Node 1:**
```bash
cargo run --release -- 1
```

**Terminal 2 - Node 2:**
```bash
cargo run --release -- 2
```

**Terminal 3 - Node 3:**
```bash
cargo run --release -- 3
```

You should see output like:
```
Node 1 on 127.0.0.1:5001
Peers: ["127.0.0.1:5002", "127.0.0.1:5003"]
```

### 4. Watch Leader Election

Within a few seconds, one node will win the election and become leader. You'll see election timeouts and vote requests in the logs.

---

## Interacting with HealthDB

### Using netcat (nc)

Connect to any node's client port (default: 6379):

```bash
nc localhost 6379
```

### Available Commands

| Command | Syntax | Example | Response |
|---------|--------|---------|----------|
| SET | `SET <key> <value>` | `SET user alice` | `OK` |
| GET | `GET <key>` | `GET user` | `alice` or `NIL` |
| DEL | `DEL <key>` | `DEL user` | `user` or `NIL` |

### Example Session

```
$ nc localhost 6379
SET name Jacob
OK
SET role student
OK
GET name
Jacob
GET missing
NIL
DEL name
name
GET name
NIL
```

---

## Project Structure

```
healthdb/
├── src/
│   ├── main.rs          # Entry point, event loop
│   ├── store.rs         # Key-value storage
│   ├── logs.rs          # Write-Ahead Log
│   ├── server.rs        # TCP server
│   ├── clients.rs       # Client handling
│   └── raft/
│       ├── mod.rs       # Module exports
│       ├── state.rs     # Node state
│       ├── messages.rs  # RPC types
│       ├── election.rs  # Leader election
│       ├── replication.rs
│       └── rpc.rs       # Network layer
├── tests/
│   └── raft_tests.rs
├── Cargo.toml
├── README.md
└── docs/
    ├── ARCHITECTURE.md
    ├── DEVELOPMENT_JOURNAL.md
    └── GETTING_STARTED.md
```

---

## Configuration

### Node Configuration

Nodes are configured via command-line arguments:

```bash
cargo run -- <node_id>
```

The node ID determines:
- **RPC Port:** `5000 + node_id`
- **Peer List:** All other nodes (assumes 3-node cluster)

### Default Ports

| Node ID | RPC Port | Client Port |
|---------|----------|-------------|
| 1 | 5001 | 6379 |
| 2 | 5002 | 6379 |
| 3 | 5003 | 6379 |

### Timeouts

| Parameter | Default | Purpose |
|-----------|---------|---------|
| Election timeout | 150ms + (node_id × 50ms) | Time before starting election |
| Heartbeat interval | 125ms | Leader heartbeat frequency |

---

## Development Workflow

### Running Tests

```bash
cargo test
```

### Running with Debug Output

```bash
RUST_LOG=debug cargo run -- 1
```

### Checking Code

```bash
cargo check   # Fast syntax/type check
cargo clippy  # Linter recommendations
```

### Formatting

```bash
cargo fmt
```

---

## Troubleshooting

### "Address already in use"

A previous instance is still running. Kill it:

```bash
pkill -f "healthdb"
# or
lsof -i :5001  # Find the process
kill <PID>     # Kill it
```

### Nodes Not Electing a Leader

1. Ensure all three nodes are running
2. Check that ports 5001-5003 are not blocked
3. Look for error messages in the terminal output

### "Connection refused"

The target node isn't running or isn't listening yet. Ensure all nodes are started.

### Compilation Errors

```bash
cargo clean
cargo build
```

If issues persist, check Rust version:
```bash
rustup update stable
```

---

## Next Steps

After getting the cluster running:

1. **Read the Architecture Docs:** Understand how the pieces fit together
2. **Explore the Code:** Start with `main.rs` and follow the data flow
3. **Run Tests:** `cargo test` to see the test suite
4. **Make Changes:** Try modifying timeout values or adding log statements

---

## Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/my-feature`
3. Make changes and test: `cargo test`
4. Commit: `git commit -m "Add my feature"`
5. Push: `git push origin feature/my-feature`
6. Open a Pull Request

---

## Getting Help

- **Issues:** Open a GitHub issue for bugs or questions
- **Discussions:** Use GitHub Discussions for general questions
- **Code Review:** PRs are welcome—feedback will be provided

---

*Happy hacking! 🦀*
