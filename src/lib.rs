//! Internal implementation shared by Vise's binary and tests.
//!
//! The entire Rust library surface, including public modules, types, fields,
//! variants, constants, functions, methods, and trait implementations, is internal
//! and unstable throughout 0.x. It is not a supported embedding API.
//!
//! [`logging::init`] installs Vise's protected stderr logger. Its protection
//! covers that initializer's output, not independently configured host logging
//! sinks. Opening a manager does not initialize tracing.

pub mod checksum;
pub mod cli;
pub mod config;
pub mod database;
pub mod engine;
pub mod error;
pub mod events;
pub mod input;
pub mod logging;
pub mod model;
pub mod net;
mod paths;
pub mod proxy;
pub mod rate_limiter;
pub mod telemetry;
pub mod web;
