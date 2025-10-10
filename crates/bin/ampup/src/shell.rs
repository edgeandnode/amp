use std::path::PathBuf;

use anyhow::{Context, Result};
use fs_err as fs;

use crate::ui;

#[derive(Debug)]
pub enum ShellError {
    ShellNotDetected,
}

impl std::fmt::Display for ShellError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ShellNotDetected => {
                writeln!(f, "Could not detect shell type")?;
                writeln!(f)?;
                writeln!(
                    f,
                    "  The SHELL environment variable is not set or is unsupported."
                )?;
                writeln!(f, "  Supported shells: bash, zsh, fish, ash")?;
                writeln!(f)?;
                writeln!(
                    f,
                    "  You can manually add the following to your shell profile:"
                )?;
                writeln!(f, "    export PATH=\"$PATH:$HOME/.amp/bin\"")?;
            }
        }
        Ok(())
    }
}

impl std::error::Error for ShellError {}

#[derive(Debug, Clone, Copy)]
pub enum Shell {
    Zsh,
    Bash,
    Fish,
    Ash,
}

impl Shell {
    /// Detect the current shell from the SHELL environment variable
    pub fn detect() -> Option<Self> {
        let shell = std::env::var("SHELL").ok()?;

        if shell.ends_with("/zsh") {
            Some(Shell::Zsh)
        } else if shell.ends_with("/bash") {
            Some(Shell::Bash)
        } else if shell.ends_with("/fish") {
            Some(Shell::Fish)
        } else if shell.ends_with("/ash") {
            Some(Shell::Ash)
        } else {
            None
        }
    }

    /// Get the profile file path for this shell
    pub fn profile_path(&self) -> Result<PathBuf> {
        let home = std::env::var("HOME")
            .or_else(|_| std::env::var("USERPROFILE"))
            .context("Could not determine home directory")?;

        let path = match self {
            Shell::Zsh => {
                let zdotdir = std::env::var("ZDOTDIR").unwrap_or(home);
                PathBuf::from(zdotdir).join(".zshenv")
            }
            Shell::Bash => PathBuf::from(home).join(".bashrc"),
            Shell::Fish => PathBuf::from(home).join(".config/fish/config.fish"),
            Shell::Ash => PathBuf::from(home).join(".profile"),
        };

        Ok(path)
    }

    /// Get the PATH export line for this shell
    pub fn path_export_line(&self, bin_dir: &str) -> String {
        match self {
            Shell::Fish => format!("fish_add_path -a {}", bin_dir),
            _ => format!("export PATH=\"$PATH:{}\"", bin_dir),
        }
    }
}

/// Add a directory to PATH by modifying the shell profile
pub fn add_to_path(bin_dir: &str) -> Result<()> {
    let shell = Shell::detect().ok_or(ShellError::ShellNotDetected)?;
    let profile_path = shell.profile_path()?;
    let export_line = shell.path_export_line(bin_dir);

    // Ensure parent directory exists
    if let Some(parent) = profile_path.parent() {
        fs::create_dir_all(parent).context("Failed to create profile directory")?;
    }

    // Read existing content or start with empty string
    let content = if profile_path.exists() {
        fs::read_to_string(&profile_path).context("Failed to read shell profile")?
    } else {
        String::new()
    };

    // Check if already in PATH
    if content.contains(&export_line) {
        ui::detail!("{} already in PATH", bin_dir);
        return Ok(());
    }

    // Append to file
    let new_content = if content.is_empty() {
        format!("{}\n", export_line)
    } else if content.ends_with('\n') {
        format!("{}\n{}\n", content.trim_end(), export_line)
    } else {
        format!("{}\n\n{}\n", content, export_line)
    };

    fs::write(&profile_path, new_content).context("Failed to write to shell profile")?;

    ui::success!(
        "Added {} to PATH in {}",
        bin_dir,
        ui::path(profile_path.display())
    );
    ui::detail!(
        "Run 'source {}' or start a new terminal session",
        profile_path.display()
    );

    Ok(())
}
