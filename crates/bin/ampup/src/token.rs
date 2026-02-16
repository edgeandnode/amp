use std::process::Command;

/// Resolve a GitHub token using the following fallback chain:
///
/// 1. Explicit token passed via `--github-token` flag or `GITHUB_TOKEN` env var
/// 2. Token from `gh auth token` (GitHub CLI)
/// 3. `None` (unauthenticated — lower rate limits)
pub fn resolve_github_token(explicit: Option<String>) -> Option<String> {
    if explicit.is_some() {
        return explicit;
    }

    try_gh_auth_token()
}

/// Attempt to retrieve a token from the GitHub CLI.
///
/// Runs `gh auth token` as a subprocess. Returns `None` on any failure:
/// `gh` not installed, not logged in, timeout, etc.
fn try_gh_auth_token() -> Option<String> {
    let output = Command::new("gh")
        .args(["auth", "token"])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .output()
        .ok()?;

    if !output.status.success() {
        return None;
    }

    let token = String::from_utf8(output.stdout).ok()?.trim().to_string();

    if token.is_empty() {
        return None;
    }

    Some(token)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_token_takes_priority() {
        let result = resolve_github_token(Some("my-explicit-token".to_string()));
        assert_eq!(result, Some("my-explicit-token".to_string()));
    }

    #[test]
    fn none_explicit_falls_through() {
        // This test verifies the fallback path is exercised.
        // The result depends on whether `gh` is installed and authenticated,
        // so we only check that it doesn't panic.
        let _ = resolve_github_token(None);
    }
}
