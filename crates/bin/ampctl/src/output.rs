//! Output formatting utilities for ampctl commands.

use std::fmt::Display;

use serde::Serialize;

/// Output format for command results.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OutputFormat {
    /// Human-readable output with colors and formatting
    #[default]
    Human,
    /// Machine-readable JSON output
    Json,
}

impl std::fmt::Display for OutputFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OutputFormat::Human => write!(f, "human"),
            OutputFormat::Json => write!(f, "json"),
        }
    }
}

impl OutputFormat {
    /// Print output in the specified format with Display implementation.
    ///
    /// In JSON mode, serializes to compact JSON on one line.
    /// In Human mode, uses Display trait for human-readable output.
    ///
    /// # Errors
    ///
    /// Returns an error if JSON serialization fails.
    pub fn print<T>(&self, data: &T) -> Result<(), serde_json::Error>
    where
        T: Serialize + Display,
    {
        match self {
            OutputFormat::Json => {
                let json = serde_json::to_string(data)?;
                println!("{}", json);
            }
            OutputFormat::Human => {
                println!("{}", data);
            }
        }
        Ok(())
    }
}
