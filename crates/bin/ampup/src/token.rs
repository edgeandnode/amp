use std::process::Command;

/// Resolve a GitHub token using the following fallback chain:
///
/// 1. Explicit token passed via `--github-token` flag or `GITHUB_TOKEN` env var
/// 2. Token from `gh auth token` (GitHub CLI)
/// 3. `None` (unauthenticated — lower rate limits)
///
/// Note: `--github-token` values may be visible in process listings (`ps aux`).
/// Prefer `GITHUB_TOKEN` env var or `gh auth token` for sensitive environments.
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
    fn resolve_github_token_with_explicit_token_returns_explicit() {
        //* Given
        let explicit = Some("my-explicit-token".to_string());

        //* When
        let result = resolve_github_token(explicit);

        //* Then
        assert_eq!(
            result,
            Some("my-explicit-token".to_string()),
            "should return the explicit token without falling through to gh CLI"
        );
    }

    #[test]
    fn resolve_github_token_with_none_exercises_fallback_without_panicking() {
        //* Given — no explicit token provided

        //* When — the result depends on whether `gh` is installed and
        //* authenticated, so we only assert it doesn't panic.
        let _result = resolve_github_token(None);

        //* Then — reaching this point means the fallback chain completed
    }
}
