use console::style;

/// Print a success message with a green checkmark
pub fn success(msg: impl std::fmt::Display) {
    println!("{} {}", style("✓").green().bold(), msg);
}

/// Print an info message with a cyan arrow
pub fn info(msg: impl std::fmt::Display) {
    println!("{} {}", style("→").cyan(), msg);
}

/// Print a warning message with a yellow warning symbol
pub fn warn(msg: impl std::fmt::Display) {
    eprintln!("{} {}", style("⚠").yellow().bold(), msg);
}

/// Print an error message with a red X
pub fn error(msg: impl std::fmt::Display) {
    eprintln!("{} {}", style("✗").red().bold(), msg);
}

/// Print a dimmed detail message (indented)
pub fn detail(msg: impl std::fmt::Display) {
    println!("  {}", style(msg).dim());
}

/// Style a version string (bold white)
pub fn version(v: impl std::fmt::Display) -> String {
    style(v).bold().to_string()
}

/// Style a path (cyan)
pub fn path(p: impl std::fmt::Display) -> String {
    style(p).cyan().to_string()
}
