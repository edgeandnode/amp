//! PostgreSQL process management
//!
//! This module provides direct management of system PostgreSQL binaries (`initdb`, `postgres`)
//! for persistent database instances. Unlike pgtemp, this implementation:
//!
//! - Accepts explicit data directory paths
//! - Reuses existing data directories (skips initdb if `PG_VERSION` exists)
//! - Uses Unix sockets for connections (no port conflicts)
//! - Detects and cleans up orphan processes from previous runs
//! - Verifies database accepts connections before returning
//!
//! # Prerequisites
//!
//! PostgreSQL must be installed on the system with `initdb`, `postgres`, and `pg_isready`
//! binaries available in PATH.

use std::{
    path::{Path, PathBuf},
    process::Stdio,
};

use tokio::{
    process::{Child, Command},
    time::{Duration, sleep},
};

use crate::error::PostgresError;

/// PostgreSQL socket filename pattern
const SOCKET_FILENAME: &str = ".s.PGSQL.5432";

/// Default timeout for PostgreSQL to become ready (seconds)
const DEFAULT_READINESS_TIMEOUT_SECS: u64 = 30;

/// Interval between readiness checks (milliseconds)
const READINESS_CHECK_INTERVAL_MS: u64 = 100;

/// Postmaster PID file name
const POSTMASTER_PID_FILE: &str = "postmaster.pid";

/// A running PostgreSQL server process
///
/// This struct manages the lifecycle of a PostgreSQL server process. The server
/// is automatically shut down when this struct is dropped.
///
/// # Example
///
/// ```ignore
/// let process = PostgresProcess::start(PathBuf::from(".amp/metadb")).await?;
/// println!("Connected via: {}", process.connection_url());
/// // Server runs until process is dropped
/// ```
pub struct PostgresProcess {
    /// The postgres server child process
    child: Child,
    /// Path to the data directory
    data_dir: PathBuf,
    /// Connection URL for this instance
    connection_url: String,
}

impl PostgresProcess {
    /// Starts a PostgreSQL server with the given data directory
    ///
    /// If the data directory does not exist or is empty, `initdb` is run to initialize it.
    /// If a `PG_VERSION` file exists, the existing data is reused.
    ///
    /// # Arguments
    ///
    /// * `data_dir` - Path where PostgreSQL will store its data files and socket
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - PostgreSQL binaries are not found in PATH
    /// - Data directory cannot be created or initialized
    /// - PostgreSQL server fails to start
    /// - Server doesn't become ready within the timeout
    pub async fn start(data_dir: PathBuf) -> Result<Self, PostgresError> {
        tracing::info!(
            data_dir = %data_dir.display(),
            "Starting PostgreSQL database service"
        );

        // Ensure data directory exists with proper permissions
        ensure_data_dir(&data_dir).await?;

        // Check for orphan postgres process and clean up if needed
        cleanup_orphan_process(&data_dir).await?;

        // Initialize if needed (check for PG_VERSION)
        let is_fresh = !is_initialized(&data_dir).await;
        if is_fresh {
            init_data_dir(&data_dir).await?;
        } else {
            tracing::info!(
                data_dir = %data_dir.display(),
                "Reusing existing PostgreSQL data directory"
            );
        }

        // Log PostgreSQL version
        log_postgres_version().await;

        // Start the postgres server
        let child = start_postgres(&data_dir).await?;

        // Build connection URL using Unix socket
        let connection_url = build_connection_url(&data_dir);

        let mut process = Self {
            child,
            data_dir,
            connection_url,
        };

        // Wait for server to be ready (socket file + accepting connections)
        process.wait_for_ready().await?;

        tracing::info!(
            data_dir = %process.data_dir.display(),
            url = %process.connection_url,
            fresh_database = is_fresh,
            "PostgreSQL database ready"
        );

        Ok(process)
    }

    /// Returns the connection URL for this PostgreSQL instance
    ///
    /// The URL uses Unix socket connections in the format:
    /// `postgresql:///postgres?host=/path/to/data_dir`
    pub fn connection_url(&self) -> &str {
        &self.connection_url
    }

    /// Returns the path to the data directory
    #[allow(dead_code)] // Part of public API, may be used by callers
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// Waits for the PostgreSQL server to become ready
    ///
    /// This checks:
    /// 1. Socket file appears in the data directory
    /// 2. `pg_isready` confirms the server is accepting connections
    async fn wait_for_ready(&mut self) -> Result<(), PostgresError> {
        let socket_path = self.data_dir.join(SOCKET_FILENAME);
        let timeout = Duration::from_secs(DEFAULT_READINESS_TIMEOUT_SECS);
        let check_interval = Duration::from_millis(READINESS_CHECK_INTERVAL_MS);
        let start = std::time::Instant::now();

        tracing::debug!(
            socket_path = %socket_path.display(),
            timeout_secs = DEFAULT_READINESS_TIMEOUT_SECS,
            "Waiting for PostgreSQL to accept connections"
        );

        loop {
            // Check if process has exited unexpectedly
            if let Some(status) = self
                .child
                .try_wait()
                .map_err(|err| PostgresError::StartFailed { source: err })?
            {
                return Err(PostgresError::UnexpectedExit {
                    status: status.code(),
                });
            }

            // Check if socket file exists first (faster than pg_isready)
            if socket_path.exists() {
                // Verify server is actually accepting connections
                if check_connection_ready(&self.data_dir).await {
                    tracing::debug!(
                        elapsed_ms = start.elapsed().as_millis() as u64,
                        "PostgreSQL is accepting connections"
                    );
                    return Ok(());
                }
            }

            // Check timeout
            if start.elapsed() >= timeout {
                // Try to kill the process since it didn't start properly
                let _ = self.child.kill().await;
                return Err(PostgresError::ReadinessTimeout {
                    timeout_secs: DEFAULT_READINESS_TIMEOUT_SECS,
                });
            }

            sleep(check_interval).await;
        }
    }

    /// Shuts down the PostgreSQL server gracefully
    ///
    /// Sends SIGTERM to the postgres process and waits for it to exit.
    /// This is called automatically when the struct is dropped, but can be
    /// called explicitly for more control.
    pub async fn shutdown(mut self) -> Result<(), PostgresError> {
        self.shutdown_inner().await
    }

    async fn shutdown_inner(&mut self) -> Result<(), PostgresError> {
        tracing::info!(
            data_dir = %self.data_dir.display(),
            "Shutting down PostgreSQL server"
        );

        // First try graceful shutdown with SIGTERM via kill command
        #[cfg(unix)]
        if let Some(pid) = self.child.id() {
            // Send SIGTERM for graceful shutdown using kill command
            let _ = Command::new("kill")
                .arg("-TERM")
                .arg(pid.to_string())
                .output()
                .await;
        }

        // Wait for process to exit (with timeout)
        let timeout = Duration::from_secs(10);
        let result = tokio::time::timeout(timeout, self.child.wait()).await;

        match result {
            Ok(Ok(status)) => {
                tracing::info!(
                    status = ?status,
                    "PostgreSQL server shut down"
                );
                Ok(())
            }
            Ok(Err(err)) => {
                tracing::warn!(error = %err, "Error waiting for PostgreSQL to exit");
                Err(PostgresError::ShutdownFailed { source: err })
            }
            Err(_) => {
                // Timeout - force kill
                tracing::warn!("PostgreSQL shutdown timed out, forcing kill");
                self.child
                    .kill()
                    .await
                    .map_err(|err| PostgresError::ShutdownFailed { source: err })?;
                Ok(())
            }
        }
    }
}

impl Drop for PostgresProcess {
    fn drop(&mut self) {
        // Best-effort shutdown - we can't await in Drop, use start_kill for SIGKILL
        // Note: This is a forceful kill; prefer calling shutdown() explicitly
        let _ = self.child.start_kill();
    }
}

/// Finds a binary in PATH by checking common PostgreSQL installation locations
/// and falling back to PATH resolution
async fn find_binary(name: &str) -> Result<PathBuf, PostgresError> {
    // Common PostgreSQL binary locations on different systems
    let common_paths = [
        // Linux package managers
        "/usr/lib/postgresql/18/bin",
        "/usr/lib/postgresql/17/bin",
        "/usr/lib/postgresql/16/bin",
        "/usr/lib/postgresql/15/bin",
        "/usr/pgsql-18/bin",
        "/usr/pgsql-17/bin",
        "/usr/pgsql-16/bin",
        // macOS Homebrew
        "/opt/homebrew/opt/postgresql@18/bin",
        "/opt/homebrew/opt/postgresql@17/bin",
        "/opt/homebrew/opt/postgresql@16/bin",
        "/opt/homebrew/opt/postgresql/bin",
        "/usr/local/opt/postgresql@18/bin",
        "/usr/local/opt/postgresql@17/bin",
        "/usr/local/opt/postgresql@16/bin",
        "/usr/local/opt/postgresql/bin",
        // macOS Postgres.app
        "/Applications/Postgres.app/Contents/Versions/latest/bin",
        // Generic system paths
        "/usr/bin",
        "/usr/local/bin",
    ];

    // Check common paths first
    for dir in common_paths {
        let path = Path::new(dir).join(name);
        if path.exists() {
            return Ok(path);
        }
    }

    // Try to find via `which` command as fallback
    let output = Command::new("which")
        .arg(name)
        .output()
        .await
        .map_err(|_| PostgresError::BinaryNotFound {
            name: name.to_string(),
        })?;

    if output.status.success() {
        let path_str = String::from_utf8_lossy(&output.stdout);
        let path = PathBuf::from(path_str.trim());
        if path.exists() {
            return Ok(path);
        }
    }

    Err(PostgresError::BinaryNotFound {
        name: name.to_string(),
    })
}

/// Ensures the data directory exists with proper permissions
async fn ensure_data_dir(data_dir: &Path) -> Result<(), PostgresError> {
    if !data_dir.exists() {
        tokio::fs::create_dir_all(data_dir)
            .await
            .map_err(|err| PostgresError::CreateDataDir {
                path: data_dir.to_path_buf(),
                source: err,
            })?;
    }

    // Set directory permissions to 700 (required by PostgreSQL)
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let permissions = std::fs::Permissions::from_mode(0o700);
        tokio::fs::set_permissions(data_dir, permissions)
            .await
            .map_err(|err| PostgresError::SetPermissions {
                path: data_dir.to_path_buf(),
                source: err,
            })?;
    }

    Ok(())
}

/// Checks if the data directory has been initialized (PG_VERSION exists)
async fn is_initialized(data_dir: &Path) -> bool {
    data_dir.join("PG_VERSION").exists()
}

/// Initializes a new PostgreSQL data directory using initdb
async fn init_data_dir(data_dir: &Path) -> Result<(), PostgresError> {
    let initdb_path = find_binary("initdb").await?;

    tracing::info!(
        data_dir = %data_dir.display(),
        initdb = %initdb_path.display(),
        "Initializing PostgreSQL data directory"
    );

    let output = Command::new(&initdb_path)
        .arg("-D")
        .arg(data_dir)
        .arg("--locale=C")
        .arg("--encoding=UTF8")
        .arg("--auth=trust") // Local connections only, no password needed
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await
        .map_err(|err| PostgresError::InitDbFailed {
            data_dir: data_dir.to_path_buf(),
            source: err,
        })?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).to_string();
        return Err(PostgresError::InitDbExitError {
            status: output.status.code().unwrap_or(-1),
            stderr,
        });
    }

    tracing::info!(
        data_dir = %data_dir.display(),
        "PostgreSQL data directory initialized"
    );

    Ok(())
}

/// Starts the postgres server process
async fn start_postgres(data_dir: &Path) -> Result<Child, PostgresError> {
    let postgres_path = find_binary("postgres").await?;

    tracing::info!(
        data_dir = %data_dir.display(),
        postgres = %postgres_path.display(),
        "Starting PostgreSQL server"
    );

    // Start postgres with:
    // - Unix socket in data directory (no TCP)
    // - Minimal logging to stderr
    let child = Command::new(&postgres_path)
        .arg("-D")
        .arg(data_dir)
        .arg("-k")
        .arg(data_dir) // Unix socket directory
        .arg("-h")
        .arg("") // Disable TCP listening
        .arg("-F") // Disable fsync for faster startup (dev mode)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|err| PostgresError::StartFailed { source: err })?;

    Ok(child)
}

/// Checks for and cleans up any orphan PostgreSQL process from a previous run
///
/// An orphan process can occur if the previous ampd instance crashed without
/// shutting down PostgreSQL properly. This function:
/// 1. Checks for postmaster.pid file
/// 2. If found, reads the PID and checks if process is still running
/// 3. If running, terminates the orphan process
/// 4. Cleans up stale socket and pid files
async fn cleanup_orphan_process(data_dir: &Path) -> Result<(), PostgresError> {
    let pid_path = data_dir.join(POSTMASTER_PID_FILE);
    let socket_path = data_dir.join(SOCKET_FILENAME);

    if !pid_path.exists() {
        return Ok(());
    }

    // Read the PID from postmaster.pid (first line)
    let pid_content = match tokio::fs::read_to_string(&pid_path).await {
        Ok(content) => content,
        Err(err) => {
            tracing::debug!(
                path = %pid_path.display(),
                error = %err,
                "Failed to read postmaster.pid, attempting cleanup"
            );
            // Try to remove stale files anyway
            let _ = tokio::fs::remove_file(&pid_path).await;
            let _ = tokio::fs::remove_file(&socket_path).await;
            return Ok(());
        }
    };

    let Some(pid_str) = pid_content.lines().next() else {
        tracing::debug!("Empty postmaster.pid file, cleaning up");
        let _ = tokio::fs::remove_file(&pid_path).await;
        let _ = tokio::fs::remove_file(&socket_path).await;
        return Ok(());
    };

    let pid: u32 = match pid_str.trim().parse() {
        Ok(pid) => pid,
        Err(_) => {
            tracing::debug!(
                pid_str = pid_str,
                "Invalid PID in postmaster.pid, cleaning up stale files"
            );
            let _ = tokio::fs::remove_file(&pid_path).await;
            let _ = tokio::fs::remove_file(&socket_path).await;
            return Ok(());
        }
    };

    // Check if process is still running using kill -0
    let process_exists = Command::new("kill")
        .arg("-0")
        .arg(pid.to_string())
        .output()
        .await
        .map(|output| output.status.success())
        .unwrap_or(false);

    if process_exists {
        tracing::warn!(
            pid = pid,
            data_dir = %data_dir.display(),
            "Detected orphan PostgreSQL process, terminating"
        );

        // Send SIGTERM to orphan process
        let _ = Command::new("kill")
            .arg("-TERM")
            .arg(pid.to_string())
            .output()
            .await;

        // Wait briefly for process to exit
        for _ in 0..50 {
            // 5 seconds max
            sleep(Duration::from_millis(100)).await;
            let still_running = Command::new("kill")
                .arg("-0")
                .arg(pid.to_string())
                .output()
                .await
                .map(|output| output.status.success())
                .unwrap_or(false);

            if !still_running {
                tracing::info!(pid = pid, "Orphan PostgreSQL process terminated");
                break;
            }
        }

        // Force kill if still running
        let still_running = Command::new("kill")
            .arg("-0")
            .arg(pid.to_string())
            .output()
            .await
            .map(|output| output.status.success())
            .unwrap_or(false);

        if still_running {
            tracing::warn!(pid = pid, "Orphan process not responding, force killing");
            let _ = Command::new("kill")
                .arg("-9")
                .arg(pid.to_string())
                .output()
                .await;
            sleep(Duration::from_millis(100)).await;
        }
    } else {
        tracing::debug!(
            pid = pid,
            "Stale postmaster.pid found (process not running), cleaning up"
        );
    }

    // Clean up stale files
    let _ = tokio::fs::remove_file(&pid_path).await;
    let _ = tokio::fs::remove_file(&socket_path).await;

    Ok(())
}

/// Checks if PostgreSQL is accepting connections using pg_isready
///
/// Returns true if the server is ready to accept connections, false otherwise.
async fn check_connection_ready(data_dir: &Path) -> bool {
    let pg_isready_path = match find_binary("pg_isready").await {
        Ok(path) => path,
        Err(_) => {
            // Fall back to socket existence check if pg_isready not found
            tracing::debug!("pg_isready not found, using socket check only");
            return data_dir.join(SOCKET_FILENAME).exists();
        }
    };

    let output = Command::new(&pg_isready_path)
        .arg("-h")
        .arg(data_dir)
        .arg("-q") // Quiet mode, exit status only
        .output()
        .await;

    match output {
        Ok(output) => output.status.success(),
        Err(_) => false,
    }
}

/// Logs the PostgreSQL version for diagnostics
async fn log_postgres_version() {
    let postgres_path = match find_binary("postgres").await {
        Ok(path) => path,
        Err(_) => return,
    };

    let output = Command::new(&postgres_path).arg("--version").output().await;

    if let Ok(output) = output
        && output.status.success()
    {
        let version = String::from_utf8_lossy(&output.stdout);
        tracing::info!(version = %version.trim(), "PostgreSQL version");
    }
}

/// Builds a PostgreSQL connection URL using Unix socket
fn build_connection_url(data_dir: &Path) -> String {
    // URL-encode the path for the host parameter
    let host_path = data_dir.to_string_lossy();
    format!("postgresql:///postgres?host={}", host_path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_connection_url() {
        let url = build_connection_url(Path::new("/tmp/test-db"));
        assert_eq!(url, "postgresql:///postgres?host=/tmp/test-db");
    }

    #[test]
    fn test_build_connection_url_with_spaces() {
        let url = build_connection_url(Path::new("/tmp/test db"));
        assert_eq!(url, "postgresql:///postgres?host=/tmp/test db");
    }
}
