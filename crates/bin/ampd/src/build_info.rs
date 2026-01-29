/// Constructs build info from compile-time environment variables.
pub fn load() -> amp_config::build_info::BuildInfo {
    amp_config::build_info::BuildInfo {
        version: env!("VERGEN_GIT_DESCRIBE").to_string(),
        commit_sha: env!("VERGEN_GIT_SHA").to_string(),
        commit_timestamp: env!("VERGEN_GIT_COMMIT_TIMESTAMP").to_string(),
        build_date: env!("VERGEN_BUILD_DATE").to_string(),
    }
}
