use anyhow::Result;

#[derive(Debug)]
pub enum PlatformError {
    UnsupportedPlatform { detected: String },
    UnsupportedArchitecture { detected: String },
}

impl std::fmt::Display for PlatformError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnsupportedPlatform { detected } => {
                writeln!(f, "Unsupported platform")?;
                writeln!(f, "  Detected: {}", detected)?;
                writeln!(f, "  Supported: linux, macos")?;
                writeln!(f)?;
                writeln!(
                    f,
                    "  If you're on a supported platform, this may be a detection issue."
                )?;
                writeln!(f, "  Try using --platform flag to override (linux, darwin)")?;
            }
            Self::UnsupportedArchitecture { detected } => {
                writeln!(f, "Unsupported architecture")?;
                writeln!(f, "  Detected: {}", detected)?;
                writeln!(f, "  Supported: x86_64, aarch64 (arm64)")?;
                writeln!(f)?;
                writeln!(
                    f,
                    "  If you're on a supported architecture, this may be a detection issue."
                )?;
                writeln!(f, "  Try using --arch flag to override (x86_64, aarch64)")?;
            }
        }
        Ok(())
    }
}

impl std::error::Error for PlatformError {}

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
            os => Err(PlatformError::UnsupportedPlatform {
                detected: os.to_string(),
            }
            .into()),
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
            arch => Err(PlatformError::UnsupportedArchitecture {
                detected: arch.to_string(),
            }
            .into()),
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

#[cfg(test)]
mod tests {
    use super::*;

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
