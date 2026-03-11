//! File format definitions for static datasets.

/// Supported file formats for static dataset tables.
///
/// Each variant carries format-specific configuration. The enum is internally
/// tagged by `"format"` so that it flattens into the parent [`Table`](super::manifest::Table)
/// struct, producing a flat JSON shape:
///
/// ```json
/// { "format": "csv", "has_header": true, ... }
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(tag = "format", rename_all = "lowercase")]
pub enum FileFormat {
    /// Comma-separated values format.
    Csv {
        /// Whether the CSV file includes a header row.
        has_header: bool,
    },
}

impl FileFormat {
    /// Returns whether the data file includes a header row.
    pub fn has_header(&self) -> bool {
        match self {
            FileFormat::Csv { has_header } => *has_header,
        }
    }
}

impl std::fmt::Display for FileFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FileFormat::Csv { .. } => write!(f, "csv"),
        }
    }
}
