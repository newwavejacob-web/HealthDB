# Contributing to HealthDB

Thank you for your interest in contributing to HealthDB! This document provides guidelines and information for contributors.

---

## Code of Conduct

Be respectful, inclusive, and constructive. We're all here to learn and build something cool.

---

## How to Contribute

### Reporting Bugs

1. Check existing issues to avoid duplicates
2. Open a new issue with:
   - Clear title describing the bug
   - Steps to reproduce
   - Expected vs actual behavior
   - Rust version and OS

### Suggesting Features

1. Open an issue with the `enhancement` label
2. Describe the feature and its use case
3. Discuss implementation approaches

### Submitting Code

1. Fork the repository
2. Create a feature branch from `main`
3. Make your changes
4. Run tests: `cargo test`
5. Run linter: `cargo clippy`
6. Format code: `cargo fmt`
7. Submit a pull request

---

## Development Setup

```bash
# Clone your fork
git clone https://github.com/YOUR_USERNAME/healthdb.git
cd healthdb

# Add upstream remote
git remote add upstream https://github.com/ORIGINAL_OWNER/healthdb.git

# Create a branch
git checkout -b feature/my-feature

# Build and test
cargo build
cargo test
```

---

## Code Style

### General Guidelines

- Follow Rust idioms and conventions
- Use meaningful variable and function names
- Add comments for complex logic
- Keep functions focused and small

### Formatting

All code must pass `cargo fmt`:

```bash
cargo fmt --check  # Check formatting
cargo fmt          # Auto-format
```

### Linting

All code should pass `cargo clippy`:

```bash
cargo clippy -- -D warnings
```

### Documentation

- Document public APIs with doc comments
- Update README if adding new features
- Add examples where helpful

```rust
/// Handles an incoming AppendEntries RPC from the leader.
/// 
/// # Arguments
/// * `state` - Mutable reference to the node's state
/// * `req` - The AppendEntries message
/// 
/// # Returns
/// An AppendEntriesResponse indicating success or failure
pub fn handle_append_entries(state: &mut NodeState, req: AppendEntriesMsg) -> AppendEntriesResponse {
    // ...
}
```

---

## Testing

### Running Tests

```bash
cargo test                    # All tests
cargo test test_name          # Specific test
cargo test -- --nocapture     # Show println! output
```

### Writing Tests

Add tests in the same file or in `tests/`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_append_entries_rejects_old_term() {
        let mut state = NodeState::new(1, "127.0.0.1:5001".into(), vec![]);
        state.current_term = 5;
        
        let req = AppendEntriesMsg {
            term: 3,  // Old term
            // ...
        };
        
        let resp = handle_append_entries(&mut state, req);
        assert!(!resp.success);
    }
}
```

---

## Pull Request Process

### Before Submitting

- [ ] Code compiles without warnings: `cargo build`
- [ ] All tests pass: `cargo test`
- [ ] No linter warnings: `cargo clippy`
- [ ] Code is formatted: `cargo fmt`
- [ ] Documentation updated if needed

### PR Description

Include:
- What the change does
- Why the change is needed
- How to test the change
- Any breaking changes

### Review Process

1. Maintainers will review your PR
2. Address any feedback
3. Once approved, your PR will be merged

---

## Areas for Contribution

### Good First Issues

Look for issues labeled `good first issue`:
- Documentation improvements
- Test coverage
- Small bug fixes

### Intermediate

- Implementing Raft features
- Performance optimizations
- Better error handling

### Advanced

- Snapshotting and log compaction
- Membership changes
- Linearizable reads

---

## Architecture Overview

Before contributing, familiarize yourself with:

1. **`store.rs`** - Core storage engine
2. **`logs.rs`** - Write-Ahead Log
3. **`raft/`** - Consensus module
   - `state.rs` - Node state
   - `messages.rs` - RPC types
   - `election.rs` - Leader election
   - `replication.rs` - Log replication
   - `rpc.rs` - Network layer

See `docs/ARCHITECTURE.md` for detailed explanations.

---

## Communication

- **Issues:** For bugs and feature requests
- **Pull Requests:** For code contributions
- **Discussions:** For questions and ideas

---

## License

By contributing, you agree that your contributions will be licensed under the MIT License.

---

Thank you for contributing! 🎉
