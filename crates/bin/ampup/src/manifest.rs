//! Release manifest data types module
//!
//! This module provides data types for Amp distribution manifests.

use std::collections::BTreeMap;

/// Current manifest format version
pub const MANIFEST_VERSION: &str = "1";

/// Complete release manifest structure matching JSON schema
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Manifest {
    /// Manifest format version (always "1")
    #[serde(default = "default_manifest_version")]
    pub manifest_version: String,
    /// ISO 8601 timestamp when manifest was generated
    pub date: String,
    /// Release version string (e.g., "1.0.0")
    pub version: String,
    /// GitHub repository metadata
    pub repository: Repository,
    /// Map of component names to their platform-specific builds
    pub components: BTreeMap<String, Component>,
}

fn default_manifest_version() -> String {
    MANIFEST_VERSION.to_string()
}

/// GitHub repository metadata
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Repository {
    /// GitHub repository owner (user or organization)
    pub owner: String,
    /// Repository name
    pub name: String,
    /// Full repository URL
    pub url: String,
}

/// Component with version and platform targets
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Component {
    /// Version string for this component
    pub version: String,
    /// Map of Rust target triples to binary downloads
    pub targets: BTreeMap<String, Target>,
}

/// Platform-specific binary target
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Target {
    /// Download URL for the binary
    pub url: String,
    /// SHA-256 hash with "sha256:" prefix
    pub hash: String,
    /// File size in bytes
    pub size: u64,
    /// Whether binary is notarized (macOS only). Defaults to false if not present.
    #[serde(default, skip_serializing_if = "is_false")]
    pub notarized: bool,
}

/// Helper function to check if notarized is false for skip_serializing_if
fn is_false(value: &bool) -> bool {
    !value
}

/// Parsed asset name with validated components
///
/// Format: `<component>-<os>-<arch>`
/// - component: lowercase alphanumeric with hyphens (e.g., "ampd", "ampctl")
/// - os: "linux" or "darwin"
/// - arch: "x86_64", "aarch64", or "arm64" (normalized to aarch64)
///
/// Tuple struct: `(component, os, arch)`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssetName(String, AssetOs, AssetArch);

impl std::str::FromStr for AssetName {
    type Err = ParseAssetNameError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Parse formats:
        // - Legacy: <component>-<os>-<arch> (e.g., "ampd-linux-x86_64")
        // - New: <component>-<os>-<abi>-<arch> (e.g., "ampd-linux-musl-x86_64", "ampd-windows-msvc-x86_64")
        //
        // Strategy: Extract arch from right, then try parsing OS with 2 parts, falling back to 1 part.
        // This handles both "linux" (1 part) and "linux-musl" (2 parts) OS specifications.

        // Extract architecture (rightmost segment)
        let arch_split = s.rfind('-').ok_or(ParseAssetNameError::InvalidFormat)?;
        let arch_str = &s[arch_split + 1..];
        let remaining = &s[..arch_split];

        // Try parsing as 4-part name first (component-os-abi-arch)
        // Find the second-to-last hyphen to potentially extract a 2-part OS
        if let Some(abi_split) = remaining.rfind('-') {
            let potential_abi = &remaining[abi_split + 1..];
            let before_abi = &remaining[..abi_split];

            // Check if we have another hyphen for the OS
            if let Some(os_split) = before_abi.rfind('-') {
                let potential_os = &before_abi[os_split + 1..];
                let os_abi_str = format!("{}-{}", potential_os, potential_abi);

                // Try parsing as a 2-part OS (e.g., "linux-musl", "windows-msvc")
                if let Ok(os) = os_abi_str.parse::<AssetOs>() {
                    let component = &before_abi[..os_split];

                    // Validate component
                    if !component.is_empty()
                        && component
                            .chars()
                            .next()
                            .is_some_and(|c| c.is_ascii_lowercase())
                        && component
                            .chars()
                            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
                    {
                        let arch = arch_str.parse().map_err(ParseAssetNameError::InvalidArch)?;
                        return Ok(AssetName(component.to_string(), os, arch));
                    }
                }
            }
        }

        // Fall back to 3-part name (component-os-arch)
        let os_split = remaining
            .rfind('-')
            .ok_or(ParseAssetNameError::InvalidFormat)?;
        let os_str = &remaining[os_split + 1..];
        let component = &remaining[..os_split];

        // Validate component: must start with lowercase letter, contain only lowercase/digits/hyphens
        if component.is_empty() {
            return Err(ParseAssetNameError::InvalidFormat);
        }

        let first_char = component
            .chars()
            .next()
            .ok_or(ParseAssetNameError::InvalidFormat)?;
        if !first_char.is_ascii_lowercase() {
            return Err(ParseAssetNameError::InvalidFormat);
        }

        if !component
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
        {
            return Err(ParseAssetNameError::InvalidFormat);
        }

        let os = os_str.parse().map_err(ParseAssetNameError::InvalidOs)?;
        let arch = arch_str.parse().map_err(ParseAssetNameError::InvalidArch)?;

        Ok(AssetName(component.to_string(), os, arch))
    }
}

impl AssetName {
    /// Returns the component name (e.g., "ampd", "ampctl")
    pub fn component(&self) -> &str {
        &self.0
    }

    /// Returns the operating system with ABI
    pub fn os(&self) -> AssetOs {
        self.1
    }

    /// Returns the CPU architecture
    pub fn arch(&self) -> AssetArch {
        self.2
    }

    /// Returns the Rust target triple (e.g., "x86_64-unknown-linux-gnu")
    ///
    /// This method generates the complete target triple by combining:
    /// - Architecture (from self.arch())
    /// - Vendor (inferred from OS)
    /// - OS and ABI (from self.os())
    ///
    /// Special case: Windows GNU on ARM64 uses "gnullvm" instead of "gnu"
    pub fn target(&self) -> String {
        let arch = self.2.as_str();

        match (self.1, self.2) {
            // Linux targets
            (AssetOs::LinuxGnu, _) => format!("{}-unknown-linux-gnu", arch),
            (AssetOs::LinuxMusl, _) => format!("{}-unknown-linux-musl", arch),

            // macOS targets
            (AssetOs::Darwin, _) => format!("{}-apple-darwin", arch),

            // Windows MSVC targets
            (AssetOs::WindowsMsvc, _) => format!("{}-pc-windows-msvc", arch),

            // Windows GNU targets
            (AssetOs::WindowsGnu, AssetArch::X86_64) => format!("{}-pc-windows-gnu", arch),
            (AssetOs::WindowsGnu, AssetArch::Aarch64) => format!("{}-pc-windows-gnullvm", arch),
        }
    }

    /// Returns the target vendor component (e.g., "unknown", "pc", "apple")
    ///
    /// This is useful for tools that need to parse or manipulate target triples.
    pub fn vendor(&self) -> &'static str {
        match self.1 {
            AssetOs::LinuxGnu | AssetOs::LinuxMusl => "unknown",
            AssetOs::Darwin => "apple",
            AssetOs::WindowsMsvc | AssetOs::WindowsGnu => "pc",
        }
    }

    /// Returns the target OS and ABI suffix (e.g., "linux-gnu", "windows-msvc")
    ///
    /// Special case: Windows GNU on ARM64 returns "windows-gnullvm"
    pub fn os_abi(&self) -> &'static str {
        match (self.1, self.2) {
            (AssetOs::LinuxGnu, _) => "linux-gnu",
            (AssetOs::LinuxMusl, _) => "linux-musl",
            (AssetOs::Darwin, _) => "darwin",
            (AssetOs::WindowsMsvc, _) => "windows-msvc",
            (AssetOs::WindowsGnu, AssetArch::X86_64) => "windows-gnu",
            (AssetOs::WindowsGnu, AssetArch::Aarch64) => "windows-gnullvm",
        }
    }

    /// Returns true if this is a macOS (apple-darwin) target
    pub fn is_macos(&self) -> bool {
        self.1.is_macos()
    }

    /// Returns true if this is a Windows target
    pub fn is_windows(&self) -> bool {
        self.1.is_windows()
    }

    /// Returns true if this is a Linux target
    pub fn is_linux(&self) -> bool {
        self.1.is_linux()
    }

    /// Returns true if this is a statically-linked binary (musl)
    pub fn is_static(&self) -> bool {
        self.1.is_static()
    }
}

/// CPU architecture enumeration
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AssetArch {
    /// x86_64 (AMD64) architecture
    X86_64,
    /// ARM64/AArch64 architecture
    Aarch64,
}

impl AssetArch {
    /// Returns the canonical string representation
    pub fn as_str(&self) -> &'static str {
        match self {
            AssetArch::X86_64 => "x86_64",
            AssetArch::Aarch64 => "aarch64",
        }
    }
}

impl std::str::FromStr for AssetArch {
    type Err = ParseAssetArchError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "x86_64" | "amd64" => Ok(AssetArch::X86_64), // Normalize amd64 → x86_64
            "aarch64" | "arm64" => Ok(AssetArch::Aarch64), // Normalize arm64 → aarch64
            _ => Err(ParseAssetArchError(s.to_string())),
        }
    }
}

/// Operating system with ABI/libc variant
///
/// Combines operating system with its C library or ABI variant to fully
/// specify the target platform. This maps directly to Rust target triple
/// OS+ABI components.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AssetOs {
    /// Linux with GNU libc (glibc) - dynamically linked
    LinuxGnu,
    /// Linux with musl libc - statically linked
    LinuxMusl,
    /// macOS (Darwin) with system libraries
    Darwin,
    /// Windows with MSVC toolchain
    WindowsMsvc,
    /// Windows with GNU toolchain (MinGW)
    WindowsGnu,
}

impl AssetOs {
    /// Returns the canonical string representation (for display/debugging)
    pub fn as_str(&self) -> &'static str {
        match self {
            AssetOs::LinuxGnu => "linux-gnu",
            AssetOs::LinuxMusl => "linux-musl",
            AssetOs::Darwin => "darwin",
            AssetOs::WindowsMsvc => "windows-msvc",
            AssetOs::WindowsGnu => "windows-gnu",
        }
    }

    /// Returns true if this is a statically-linked target
    pub fn is_static(&self) -> bool {
        matches!(self, AssetOs::LinuxMusl)
    }

    /// Returns true if this is a macOS target
    pub fn is_macos(&self) -> bool {
        matches!(self, AssetOs::Darwin)
    }

    /// Returns true if this is a Windows target
    pub fn is_windows(&self) -> bool {
        matches!(self, AssetOs::WindowsMsvc | AssetOs::WindowsGnu)
    }

    /// Returns true if this is a Linux target
    pub fn is_linux(&self) -> bool {
        matches!(self, AssetOs::LinuxGnu | AssetOs::LinuxMusl)
    }
}

impl std::str::FromStr for AssetOs {
    type Err = ParseAssetOsError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            // Backward compatibility: bare OS names default to common ABI
            "linux" => Ok(AssetOs::LinuxGnu),
            "darwin" => Ok(AssetOs::Darwin),

            // Explicit OS+ABI combinations
            "linux-gnu" => Ok(AssetOs::LinuxGnu),
            "linux-musl" => Ok(AssetOs::LinuxMusl),
            "windows-msvc" => Ok(AssetOs::WindowsMsvc),
            "windows-gnu" => Ok(AssetOs::WindowsGnu),

            _ => Err(ParseAssetOsError(s.to_string())),
        }
    }
}

/// Errors that occur when parsing asset names
///
/// Asset names follow the format `<component>-<os>[-<abi>]-<arch>` where:
/// - component: lowercase alphanumeric with hyphens (e.g., "ampd", "ampctl")
/// - os: Operating system, optionally with ABI (e.g., "linux", "linux-musl", "windows-msvc")
/// - arch: CPU architecture - "x86_64" or "aarch64" (or "arm64" which normalizes to "aarch64")
///
/// # Supported Formats
///
/// Legacy (backward compatible):
/// - `ampd-linux-x86_64` → defaults to GNU libc
/// - `ampd-darwin-aarch64` → macOS on Apple Silicon
///
/// Explicit OS+ABI:
/// - `ampd-linux-musl-x86_64` → static Linux binary
/// - `ampd-windows-msvc-x86_64` → Windows with MSVC
#[derive(Debug, thiserror::Error)]
pub enum ParseAssetNameError {
    /// Asset name does not match the expected format
    ///
    /// This occurs when the asset name cannot be parsed into valid components
    /// separated by hyphens, or when the component name doesn't meet validation
    /// requirements.
    ///
    /// Common causes:
    /// - Missing hyphens (e.g., "ampdlinuxx86_64")
    /// - Too few components (need at least component-os-arch)
    /// - Component name is empty
    /// - Component doesn't start with a lowercase letter
    /// - Component contains invalid characters (only lowercase, digits, hyphens allowed)
    #[error("invalid asset name format: expected '<component>-<os>[-<abi>]-<arch>'")]
    InvalidFormat,

    /// Invalid operating system name in asset filename
    ///
    /// This occurs when the OS portion is not a recognized OS or OS+ABI combination.
    /// See `ParseAssetOsError` for the list of supported values.
    #[error("invalid OS in asset name")]
    InvalidOs(#[source] ParseAssetOsError),

    /// Invalid CPU architecture name in asset filename
    ///
    /// This occurs when the architecture portion is not "x86_64", "aarch64", or "arm64".
    /// Note: "arm64" is automatically normalized to "aarch64".
    #[error("invalid architecture in asset name")]
    InvalidArch(#[source] ParseAssetArchError),
}

/// Error when parsing operating system name
///
/// Occurs when attempting to parse a string that is not a recognized OS+ABI combination.
///
/// Supported formats:
/// - Bare OS (backward compat): "linux", "darwin"
/// - OS with ABI: "linux-gnu", "linux-musl", "windows-msvc", "windows-gnu"
#[derive(Debug, thiserror::Error)]
#[error(
    "invalid OS: '{0}' (expected 'linux', 'linux-gnu', 'linux-musl', 'darwin', 'windows-msvc', or 'windows-gnu')"
)]
pub struct ParseAssetOsError(String);

/// Error when parsing CPU architecture name
///
/// Occurs when attempting to parse a string that is not a recognized architecture.
///
/// Supported architecture strings:
/// - "x86_64" or "amd64" (automatically normalized to "x86_64")
/// - "aarch64" or "arm64" (automatically normalized to "aarch64")
#[derive(Debug, thiserror::Error)]
#[error("invalid architecture: '{0}' (expected 'x86_64', 'amd64', 'aarch64', or 'arm64')")]
pub struct ParseAssetArchError(String);
