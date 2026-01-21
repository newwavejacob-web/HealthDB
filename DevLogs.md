# Development Journal

A chronicle of building HealthDB from scratch—the struggles, breakthroughs, and lessons learned.

---

## Phase 1: Concurrent Key-Value Store

**Duration:** ~3 days

### Day 1: The Beginning

Started with a "fat main.rs" containing everything. The goal was simple: build a concurrent key-value store in Rust. Coming from C with only one beginner Rust project months ago, I felt friction immediately.

**Challenge:** Modularizing the code into `store.rs`, `clients.rs`, and `server.rs`.

**Lesson:** Rust's module system is different from C's `#include`. Took time to understand `mod`, `use`, and `pub`.

### Days 2-3: Fighting the Borrow Checker

This is where Rust showed its teeth. Kept running into ownership issues when trying to share the database across threads.

**The Solution:**
```rust
pub type Database = Arc<Mutex<HashMap<String, String>>>;
```

- `Arc` = Atomic Reference Counting (safe to share across threads)
- `Mutex` = Mutual exclusion (one thread at a time)
- Combined = thread-safe shared state

**Breakthrough:** Day 3—it finally compiled! Had a working modular concurrent K/V store.

---

## Phase 2: Write-Ahead Log

**Duration:** ~6 days

### Day 4: Understanding WAL

Started researching Write-Ahead Logs. The concept is simple: write operations to a log file before applying them. If the system crashes, replay the log to recover.

**Initial Implementation:**
- Create log file if it doesn't exist ✓
- Append operations on SET/DEL ✓
- Realized GET doesn't need logging (it doesn't modify state) ✓

**Challenge:** String interoperability in Rust. The compiler does not play around with string types.

### Days 5-6: The Reload Problem

Implementing reload logic—replaying the log to restore state.

**Bug Discovered:** My code was double-logging operations:
1. During normal operation (intended)
2. During reload (unintended)

**Solution:** Added a `log_flag` parameter to control when logging happens:
```rust
pub fn set(db: &Database, key: String, value: String, log_flag: bool) {
    if log_flag {
        logs::log_set(&key, &value).unwrap();
    }
    let mut map = db.lock().unwrap();
    map.insert(key, value);
}
```

### Day 7: Architectural Decisions

Contemplated making the database a struct vs keeping it as a typed alias.

**Considered:**
```rust
// Option A: Struct
pub struct Database {
    db: Arc<Mutex<HashMap<String, String>>>,
}

// Option B: Type alias (chosen)
pub type Database = Arc<Mutex<HashMap<String, String>>>;
```

**Decision:** Kept the functional approach. Passing the database to functions felt cleaner for the end goals.

### Days 8-9: Checksums and Compilation

Added CRC32 checksums to detect log corruption:
```
Format: <byte_size> <OP> <key> [value] <checksum>
```

**The Grind:** Day 9 was a marathon. Told myself I wouldn't quit until it compiled. 24 errors later... success!

**Key Insight:** Overthinking the reload logic. We only need to reload when the database is new—it doesn't need complex state tracking.

### Day 10: Snapshots (Brief Exploration)

Started thinking about loading the HashMap directly from a file (snapshotting). Decided to move on to Raft instead.

---

## Phase 3: Raft Consensus

**Duration:** ~2 weeks

### Week 1: The Overwhelming Beginning

Raft felt like it had a hundred moving parts. The paper was dense. First week was mostly:
- Reading the Raft paper
- Trying to interpret anything into code
- Complete confusion about how nodes communicate

**The Wall:** Never worked on anything this scale. Had no idea how to get nodes talking over the wire.

### Breakthrough: Understanding Data Flow

The lightbulb moment: Raft is just actors doing their jobs and processing data while communicating.

Once I understood how data flowed through the system:
```
Request → Serialize → TCP → Deserialize → Handle → Respond
```

Everything became more manageable.

### The Async Struggle

Learning `async/await` in Rust was brutal. Coming from synchronous C, the mental model shift was significant.

**What Helped:**
- Building a simple "clock timer" (heartbeat logic) to practice
- Understanding that `await` is a yield point, not a blocking call
- Accepting that the borrow checker + async = extra complexity

### Wiring It Up

First 1.5 weeks: just trying to understand the request/response cycle.

**Questions I Had to Answer:**
- How do structs flow through TCP streams?
- How does serialization work with `bincode`?
- How do I handle async connections without blocking?

**Channels Attempt:** Tried using `mpsc` channels for concurrency. Couldn't get the advanced leader election logic to compile. Abandoned for direct async handling.

### Leader Election: The Hard Part

This is what "whoooped my ass."

**Challenges:**
1. Understanding `tokio::select!` for racing conditions
2. Implementing election timeouts
3. Handling vote requests and responses
4. The voting logic itself

**The select! Pattern:**
```rust
tokio::select! {
    _ = sleep(election_timeout) => {
        // Start election
    }
    _ = sleep(heartbeat_interval), if is_leader => {
        // Send heartbeats
    }
    result = listener.accept() => {
        // Handle incoming RPC
    }
}
```

This macro races multiple futures and executes the first one that completes. Perfect for Raft's timeout-driven behavior.

---

## Key Lessons Learned

### 1. The Borrow Checker is Your Friend

Initially frustrating, but it catches real bugs. Every fight with the borrow checker taught me something about memory safety.

### 2. Start Simple, Then Iterate

The concurrent K/V store → WAL → Raft progression was the right approach. Each layer built on the previous.

### 3. Read the Paper, Then Read It Again

The Raft paper became clearer on each read. First pass: confusion. Second pass: partial understanding. Third pass: implementation clarity.

### 4. Modularization Helps Thinking

Breaking code into modules (`state.rs`, `messages.rs`, `election.rs`, etc.) forced clear separation of concerns.

### 5. Don't Overthink

Several times I overcomplicated things. The reload logic, the struct vs type alias debate—simpler was usually better.

### 6. Async is a Different Paradigm

Coming from C, async Rust required a mental model shift. It's not threading. It's not callbacks (exactly). It's cooperative multitasking.

---

## What's Next

1. **Finish Log Replication:** Complete the commit advancement and apply logic
2. **Client Forwarding:** Redirect clients to the leader
3. **Snapshots:** Implement log compaction
4. **Benchmarking:** Measure actual performance

---

## Time Investment

| Phase | Duration | Main Challenges |
|-------|----------|-----------------|
| K/V Store | ~3 days | Module system, borrow checker |
| WAL | ~6 days | String handling, reload logic |
| Raft | ~2 weeks | Async, leader election, RPC |

**Total:** ~4 weeks of intensive learning and building

---

## Resources That Helped

- **The Raft Paper:** https://raft.github.io/raft.pdf
- **Raft Visualization:** https://raft.github.io/
- **Tokio Tutorial:** https://tokio.rs/tokio/tutorial
- **The Rust Book:** https://doc.rust-lang.org/book/
- **Stack Overflow:** For specific Rust syntax questions
- **W3Schools:** Quick TCP refresher

---

*"The struggle is part of the learning. Every compiler error taught me something."*
