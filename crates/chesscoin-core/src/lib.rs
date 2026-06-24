//! Core protocol simulator for ChessCoin.
//!
//! This crate follows a hexagonal layout:
//! - `domain` contains protocol data and deterministic transition rules.
//! - `ports` defines boundaries for replaceable infrastructure.
//! - `application` orchestrates the simulator use case.

pub mod application;
pub mod domain;
pub mod ports;
