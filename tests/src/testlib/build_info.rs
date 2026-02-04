/// Build information populated from vergen at compile time.
#[derive(Debug, Clone)]
pub struct BuildInfo {
    /// Git describe output (e.g. `v0.0.22-15-g8b065bde`)
    pub version: String,
    /// Full commit SHA hash (e.g. `8b065bde1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d`)
    pub commit_sha: String,
    /// Commit timestamp in ISO 8601 format (e.g. `2025-10-30T11:14:07Z`)
    pub commit_timestamp: String,
    /// Build date (e.g. `2025-10-30`)
    pub build_date: String,
}

/// Constructs build info from compile-time environment variables.
pub fn load() -> BuildInfo {
    BuildInfo {
        version: env!("VERGEN_GIT_DESCRIBE").to_string(),
        commit_sha: env!("VERGEN_GIT_SHA").to_string(),
        commit_timestamp: env!("VERGEN_GIT_COMMIT_TIMESTAMP").to_string(),
        build_date: env!("VERGEN_BUILD_DATE").to_string(),
    }
}
