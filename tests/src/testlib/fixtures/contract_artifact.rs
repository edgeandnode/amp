//! Contract artifact fixture for loading compiled Solidity contracts.
//!
//! This module provides utilities for loading contract artifacts produced by
//! Forge (Foundry) compilation, extracting ABI and bytecode for deployment.

use std::path::Path;

use alloy::{hex, json_abi::JsonAbi, primitives::Bytes};
use fs_err as fs;

use crate::BoxError;

/// Contract artifact loaded from a Forge compilation output JSON file.
///
/// Contains the contract's ABI and deployment bytecode extracted from
/// a forge build artifact (e.g., `out/Counter.sol/Counter.json`).
#[derive(Debug, Clone)]
pub struct ContractArtifact {
    /// The contract's ABI for encoding/decoding function calls.
    pub abi: JsonAbi,
    /// The contract's deployment bytecode.
    pub bytecode: Bytes,
}

impl ContractArtifact {
    /// Load a contract artifact from a Forge output JSON file.
    ///
    /// The artifact_path should point to a file like `out/Counter.sol/Counter.json`
    /// produced by `forge build`. Extracts the ABI and bytecode for deployment.
    pub fn load(path: &Path) -> Result<Self, BoxError> {
        tracing::debug!(path = %path.display(), "Loading contract artifact");

        // Read and parse the forge artifact JSON
        let artifact_json = fs::read_to_string(path).map_err(|err| {
            format!(
                "Failed to read artifact file at '{}': {}",
                path.display(),
                err
            )
        })?;

        let artifact: ForgeArtifact = serde_json::from_str(&artifact_json).map_err(|err| {
            format!(
                "Failed to parse artifact JSON from '{}': {}",
                path.display(),
                err
            )
        })?;

        // Decode bytecode hex
        let bytecode_hex = artifact
            .bytecode
            .object
            .strip_prefix("0x")
            .unwrap_or(&artifact.bytecode.object);
        let bytecode = Bytes::from(
            hex::decode(bytecode_hex)
                .map_err(|err| format!("Failed to decode bytecode hex: {}", err))?,
        );

        tracing::debug!(
            path = %path.display(),
            bytecode_len = bytecode.len(),
            "Contract artifact loaded successfully"
        );

        Ok(Self {
            abi: artifact.abi,
            bytecode,
        })
    }

    /// Get the function selector for a given function name.
    ///
    /// Returns the 4-byte function selector as bytes.
    /// If multiple overloaded functions exist, returns the selector for the first one.
    pub fn function_selector(&self, function_name: &str) -> Result<Bytes, BoxError> {
        let functions = self
            .abi
            .function(function_name)
            .ok_or_else(|| format!("Function '{}' not found in contract ABI", function_name))?;

        let function = functions
            .first()
            .ok_or_else(|| format!("Function '{}' exists but has no entries", function_name))?;

        let selector = function.selector();
        Ok(Bytes::from(selector.to_vec()))
    }

    /// Get the function selector for a given function name as a hex string.
    ///
    /// Returns the 4-byte function selector as a hex string (without 0x prefix).
    /// If multiple overloaded functions exist, returns the selector for the first one.
    pub fn function_selector_hex(&self, function_name: &str) -> Result<String, BoxError> {
        let selector_bytes = self.function_selector(function_name)?;
        Ok(hex::encode(selector_bytes))
    }
}

/// Forge artifact JSON structure.
///
/// Represents the JSON output format produced by `forge build`.
#[derive(Debug, serde::Deserialize)]
struct ForgeArtifact {
    abi: JsonAbi,
    bytecode: BytecodeObject,
}

/// Bytecode object from Forge artifact.
#[derive(Debug, serde::Deserialize)]
struct BytecodeObject {
    object: String,
}
