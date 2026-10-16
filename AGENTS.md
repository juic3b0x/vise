# AGENTS.md — Vise Core Directive

## Project Overview
Vise is a high-performance, industrial-grade Rust download manager engineered for raw speed, high concurrency, and bulletproof crash recovery. No filler, no toy scripts, no bloat.

## Tech Stack Mandates
- **Language:** Rust (stable, latest edition)
- **Async Runtime:** Tokio
- **HTTP Client:** Reqwest (chunked streaming)
- **WebUI Backend:** Axum
- **State Persistence:** Embedded SQLite (WAL mode, atomic transactions via `rusqlite` / `sqlx`)
- **CLI Progress:** Indicatif

## Engineering Standards
1. **Zero Bloat:** Do not pull in heavy third-party dependency crates unless it's strictly necessary. Keep the binary lean and mean.
2. **Crash-Resistant State:** All download progress and chunk offsets must commit atomically to SQLite. Never trust fragile flat-file tracking.
3. **Strict Linting & Testing:** 
   - Code must compile clean with zero warnings.
   - Must pass `cargo clippy` with zero complaints.
   - All tests (`cargo test`) must pass green before any feature is marked done.
4. **Clean Architecture:** Keep the core async download engine strictly decoupled from the CLI event loop and the Axum WebUI telemetry bridge.

## Commit Message Convention
Use conventional commit prefixes enclosed in parentheses:
- `feat(scope): description`
- `fix(scope): description`
- `refactor(scope): description`
- `test(scope): description`
