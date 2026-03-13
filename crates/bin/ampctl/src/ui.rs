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

/// Asks users if they are certain about a certain action.
pub fn prompt_for_confirmation(prompt: &str) -> Result<bool, std::io::Error> {
    use std::io::Write;

    print!("{prompt} [y/N] ");
    std::io::stdout().flush()?;

    let mut answer = String::new();
    std::io::stdin().read_line(&mut answer)?;
    answer.make_ascii_lowercase();

    match answer.trim() {
        "y" | "yes" => Ok(true),
        _ => Ok(false),
    }
}

/// Style a version string (bold white)
pub fn version(v: impl std::fmt::Display) -> String {
    style(v).bold().to_string()
}

/// Style a path (cyan)
pub fn path(p: impl std::fmt::Display) -> String {
    style(p).cyan().to_string()
}

/// Convert a `snake_case` key to `Title Case`.
pub fn snake_to_title(key: &str) -> String {
    key.split('_')
        .map(|w| {
            let mut c = w.chars();
            match c.next() {
                None => String::new(),
                Some(first) => first.to_uppercase().to_string() + c.as_str(),
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

/// Write aligned key-value fields to a formatter.
///
/// Each field is displayed as `label : value`, with labels right-padded
/// to the width of the longest label for visual alignment.
pub fn write_aligned_fields(
    f: &mut std::fmt::Formatter,
    fields: &[(String, String)],
    indent: &str,
) -> std::fmt::Result {
    let max_label_width = fields
        .iter()
        .map(|(label, _)| label.len())
        .max()
        .unwrap_or(0);

    for (label, value) in fields {
        writeln!(f, "{indent}{label:<max_label_width$} : {value}")?;
    }
    Ok(())
}

/// Extract a display-friendly string from a JSON value.
pub fn json_display_value(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// Format structured error detail in a human-readable way.
///
/// Recognizes the structured format with `error_message`, `error_code`, and
/// `error_context` fields. Prints "Unknown error" when the expected fields are absent.
pub fn format_error_detail(
    f: &mut std::fmt::Formatter,
    detail: &serde_json::Value,
) -> std::fmt::Result {
    let error_message = detail
        .get("error_message")
        .and_then(|v| v.as_str())
        .unwrap_or("Unknown error");

    writeln!(f)?;
    writeln!(f, "✖ {error_message}")?;

    let error_code = detail.get("error_code").and_then(|v| v.as_str());
    let ctx = detail.get("error_context").and_then(|v| v.as_object());

    let mut fields: Vec<(String, String)> = Vec::new();

    if let Some(code) = error_code {
        fields.push(("Error Code".to_string(), code.to_string()));
    }

    if let Some(ctx) = ctx {
        for (key, value) in ctx {
            if key == "stack_trace" {
                continue;
            }
            fields.push((snake_to_title(key), json_display_value(value)));
        }
    }

    if !fields.is_empty() {
        writeln!(f)?;
        write_aligned_fields(f, &fields, "  ")?;
    }

    // Print stack trace if present
    if let Some(traces) = ctx
        .and_then(|c| c.get("stack_trace"))
        .and_then(|v| v.as_array())
    {
        writeln!(f)?;
        writeln!(f, "  Stack Trace:")?;
        for trace in traces {
            if let Some(msg) = trace.as_str() {
                writeln!(f, "    • {msg}")?;
            }
        }
    }

    Ok(())
}
