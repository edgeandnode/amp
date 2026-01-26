//! Client creation for dataset providers.
//!
//! This module provides unified client types for different data providers:
//! - [`block_stream`] - Block streaming clients (EVM RPC, Solana, Firehose, etc.)
//! - [`evm_rpc`] - Direct EVM JSON-RPC provider access

pub mod block_stream;
pub mod evm_rpc;
