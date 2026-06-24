//! Node-side adapters and runtime for ChessCoin v0.1.
//!
//! This crate sits outside `chesscoin-core`. It owns concrete infrastructure:
//! local hashing/sampling adapters, wire encoding, TCP networking, and node
//! runtime orchestration.

pub mod adapters;
pub mod runtime;
pub mod wire;
