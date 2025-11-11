use console::style;

/// Print a success message with a green checkmark
#[macro_export]
macro_rules! success {
    ($($arg:tt)*) => {
        eprintln!("{} {}", console::style("✓").green().bold(), format!($($arg)*))
    };
}

/// Print an info message with a cyan arrow
#[macro_export]
macro_rules! info {
    ($($arg:tt)*) => {
        eprintln!("{} {}", console::style("→").cyan(), format!($($arg)*))
    };
}

/// Print a warning message with a yellow warning symbol
#[macro_export]
macro_rules! warning {
    ($($arg:tt)*) => {
        eprintln!("{} {}", console::style("⚠").yellow().bold(), format!($($arg)*))
    };
}

/// Print an error message with a red cross, including the full error chain
#[macro_export]
macro_rules! error {
    ($err:expr) => {{
        eprintln!("{} {}", console::style("✗").red().bold(), $err);

        // For anyhow::Error, iterate through the chain
        let err_ref = &$err;
        for (i, cause) in err_ref.chain().skip(1).enumerate() {
            if i == 0 {
                eprintln!(
                    "  {} {}",
                    console::style("→").dim(),
                    console::style(cause).dim()
                );
            } else {
                eprintln!(
                    "    {} {}",
                    console::style("→").dim(),
                    console::style(cause).dim()
                );
            }
        }
    }};
}

/// Print a dimmed detail message (indented)
#[macro_export]
macro_rules! detail {
    ($($arg:tt)*) => {
        eprintln!("  {}", console::style(format!($($arg)*)).dim())
    };
}

/// Style a version string (bold white)
pub fn version(v: impl std::fmt::Display) -> String {
    style(v).bold().to_string()
}

/// Style a path (cyan)
pub fn path(p: impl std::fmt::Display) -> String {
    style(p).cyan().to_string()
}
