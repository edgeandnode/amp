use anyhow::{Result, anyhow};

/// Supported platforms
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Platform {
    Linux,
    Darwin,
}

impl Platform {
    /// Detect the current platform
    pub fn detect() -> Result<Self> {
        match std::env::consts::OS {
            "linux" => Ok(Self::Linux),
            "macos" => Ok(Self::Darwin),
            os => Err(anyhow!("Unsupported platform: {}", os)),
        }
    }

    /// Get the platform string for artifact names
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Linux => "linux",
            Self::Darwin => "darwin",
        }
    }
}

impl std::fmt::Display for Platform {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Supported architectures
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Architecture {
    X86_64,
    Aarch64,
}

impl Architecture {
    /// Detect the current architecture
    pub fn detect() -> Result<Self> {
        match std::env::consts::ARCH {
            "x86_64" | "amd64" => Ok(Self::X86_64),
            "aarch64" | "arm64" => Ok(Self::Aarch64),
            arch => Err(anyhow!("Unsupported architecture: {}", arch)),
        }
    }

    /// Get the architecture string for artifact names
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::X86_64 => "x86_64",
            Self::Aarch64 => "aarch64",
        }
    }
}

impl std::fmt::Display for Architecture {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Get the artifact name for the current platform and architecture
pub fn artifact_name(platform: Platform, arch: Architecture) -> String {
    format!("nozzle-{}-{}", platform.as_str(), arch.as_str())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_artifact_name() {
        assert_eq!(
            artifact_name(Platform::Linux, Architecture::X86_64),
            "nozzle-linux-x86_64"
        );
        assert_eq!(
            artifact_name(Platform::Darwin, Architecture::Aarch64),
            "nozzle-darwin-aarch64"
        );
    }

    #[test]
    fn test_platform_detect() {
        // This will work on any supported platform
        let platform = Platform::detect();
        assert!(platform.is_ok());
    }

    #[test]
    fn test_arch_detect() {
        // This will work on any supported architecture
        let arch = Architecture::detect();
        assert!(arch.is_ok());
    }
}
