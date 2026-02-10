//! PostgreSQL process management.
//!
//! Direct management of system PostgreSQL binaries for persistent database
//! instances. Reuses existing data directories (skips `initdb` if `PG_VERSION`
//! exists), connects via Unix sockets, and detects orphan processes.
//!
//! # PostgreSQL Binaries
//!
//! This module invokes the following PostgreSQL command-line tools. All binaries
//! must be available in `PATH` (discovered via the [`which`] crate) or supplied
//! via [`PostgresBuilder::bin_path`].
//!
//! | Binary     | Role                              | Reference |
//! |------------|-----------------------------------|-----------|
//! | `initdb`   | Initialize a new database cluster | [`initdb`](https://www.postgresql.org/docs/16/app-initdb.html) |
//! | `postgres` | Run the database server           | [`postgres`](https://www.postgresql.org/docs/16/app-postgres.html) |
//!
//! # Supported Versions
//!
//! Tested with PostgreSQL 16, 17, 18. Prefer PostgreSQL 18 per issue #1383.
//! All CLI arguments used are stable across these versions.

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    process::Stdio,
};

use backon::{ExponentialBuilder, Retryable};
use percent_encoding::{AsciiSet, CONTROLS, utf8_percent_encode};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    process::{Child, Command},
    task::JoinHandle,
    time::Duration,
};

/// PostgreSQL socket filename pattern
const SOCKET_FILENAME: &str = ".s.PGSQL.5432";

/// Default timeout for PostgreSQL to become ready (seconds)
const DEFAULT_READINESS_TIMEOUT_SECS: u64 = 30;

/// Postmaster PID file name
const POSTMASTER_PID_FILE: &str = "postmaster.pid";

/// PostgreSQL shared memory implementation.
///
/// Controls how PostgreSQL allocates its main shared memory area.
/// Using `Mmap` avoids System V shared memory, which has low default
/// limits on macOS and can leak if the process is killed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SharedMemoryType {
    /// Memory-mapped files (anonymous mmap). Default since PostgreSQL 12.
    Mmap,
    /// System V shared memory (legacy).
    SysV,
}

impl std::fmt::Display for SharedMemoryType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Mmap => f.write_str("mmap"),
            Self::SysV => f.write_str("sysvipc"),
        }
    }
}

/// PostgreSQL dynamic shared memory implementation.
///
/// Controls how PostgreSQL allocates dynamic shared memory segments
/// used for parallel queries and other operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DynamicSharedMemoryType {
    /// POSIX shared memory (`shm_open`).
    Posix,
    /// System V shared memory.
    SysV,
    /// Memory-mapped files.
    Mmap,
}

impl std::fmt::Display for DynamicSharedMemoryType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Posix => f.write_str("posix"),
            Self::SysV => f.write_str("sysv"),
            Self::Mmap => f.write_str("mmap"),
        }
    }
}

/// PostgreSQL `shared_buffers` memory size.
///
/// Wraps [`byte_unit::Byte`] internally, formats to PostgreSQL memory syntax on output.
/// Constructors mirror [`std::time::Duration`] — e.g., `SharedBuffers::from_mb(128)`.
///
/// PostgreSQL accepts the units `kB` (1024), `MB` (1024^2), `GB` (1024^3).
/// [`Display`](std::fmt::Display) picks the largest unit that divides evenly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SharedBuffers(byte_unit::Byte);

impl SharedBuffers {
    /// Creates a `SharedBuffers` value from kilobytes (1 kB = 1024 bytes).
    ///
    /// # Panics
    ///
    /// Panics if `kb * 1024` overflows `u64`.
    pub fn from_kb(kb: u64) -> Self {
        Self(
            byte_unit::Byte::from_u64_with_unit(kb, byte_unit::Unit::KiB)
                .expect("kilobytes overflow"),
        )
    }

    /// Creates a `SharedBuffers` value from megabytes (1 MB = 1024² bytes).
    ///
    /// # Panics
    ///
    /// Panics if `mb * 1024 * 1024` overflows `u64`.
    pub fn from_mb(mb: u64) -> Self {
        Self(
            byte_unit::Byte::from_u64_with_unit(mb, byte_unit::Unit::MiB)
                .expect("megabytes overflow"),
        )
    }

    /// Creates a `SharedBuffers` value from gigabytes (1 GB = 1024³ bytes).
    ///
    /// # Panics
    ///
    /// Panics if `gb * 1024 * 1024 * 1024` overflows `u64`.
    pub fn from_gb(gb: u64) -> Self {
        Self(
            byte_unit::Byte::from_u64_with_unit(gb, byte_unit::Unit::GiB)
                .expect("gigabytes overflow"),
        )
    }
}

impl std::fmt::Display for SharedBuffers {
    /// Formats to PostgreSQL memory syntax, picking the largest exact binary unit.
    ///
    /// Delegates unit selection to [`byte_unit::Byte::get_exact_unit`], then maps
    /// the binary unit variant to PostgreSQL notation (`GiB` -> `GB`, `MiB` -> `MB`,
    /// `KiB` -> `kB`).
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (value, unit) = self.0.get_exact_unit(false);
        let pg_unit = match unit {
            byte_unit::Unit::GiB => "GB",
            byte_unit::Unit::MiB => "MB",
            byte_unit::Unit::KiB => "kB",
            _ => "kB",
        };
        write!(f, "{value}{pg_unit}")
    }
}

/// Builder for configuring and starting a managed PostgreSQL instance.
///
/// Provides a builder-based async API for starting a PostgreSQL server with
/// configurable locale, encoding, server runtime parameters, and binary paths.
/// Dev-mode defaults (fsync=off, etc.) are applied automatically unless overridden.
///
/// # Example
///
/// ```ignore
/// use metadata_db_postgres::PostgresBuilder;
///
/// // Minimal usage with app defaults
/// let (handle, bg_task) = PostgresBuilder::new(".amp/metadb")
///     .locale("C")
///     .start()
///     .await?;
///
/// // Fully configured
/// let (handle, bg_task) = PostgresBuilder::new(".amp/metadb")
///     .locale("C")
///     .encoding("UTF8")
///     .shared_memory_type(SharedMemoryType::Mmap)
///     .dynamic_shared_memory_type(DynamicSharedMemoryType::Posix)
///     .shared_buffers(SharedBuffers::from_mb(128))
///     .config_param("max_connections", "50")
///     .initdb_arg("--data-checksums", "")
///     .bin_path("/usr/lib/postgresql/18/bin")
///     .start()
///     .await?;
/// ```
#[derive(Debug, Clone)]
pub struct PostgresBuilder {
    data_dir: PathBuf,
    locale: Option<String>,
    encoding: Option<String>,
    server_configs: BTreeMap<String, String>,
    initdb_args: BTreeMap<String, String>,
    bin_path: Option<PathBuf>,
    wait_for_exit: bool,
}

impl PostgresBuilder {
    /// Creates a new builder for a PostgreSQL instance at the given data directory.
    ///
    /// The data directory is the only required parameter. If it does not exist,
    /// it will be created. If it already contains a `PG_VERSION` file, the existing
    /// database cluster is reused (initdb is skipped).
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        Self {
            data_dir: data_dir.into(),
            locale: None,
            encoding: None,
            server_configs: BTreeMap::new(),
            initdb_args: BTreeMap::new(),
            bin_path: None,
            wait_for_exit: true,
        }
    }

    /// Sets the locale for `initdb --locale`.
    ///
    /// Only applies when initializing a fresh data directory. Ignored if the
    /// data directory already contains a database cluster.
    #[must_use]
    pub fn locale(mut self, locale: &str) -> Self {
        self.locale = Some(locale.to_owned());
        self
    }

    /// Sets the encoding for `initdb --encoding`.
    ///
    /// Only applies when initializing a fresh data directory. Ignored if the
    /// data directory already contains a database cluster.
    #[must_use]
    pub fn encoding(mut self, encoding: &str) -> Self {
        self.encoding = Some(encoding.to_owned());
        self
    }

    /// Adds a PostgreSQL server runtime parameter, passed as `-c key=val` to the
    /// `postgres` command.
    ///
    /// If a key is set multiple times, the last value wins.
    ///
    /// # Dev-mode defaults
    ///
    /// The following non-durability parameters are applied by default for local
    /// development. Callers can override any of them:
    ///
    /// | Parameter             | Default | Rationale                               |
    /// |-----------------------|---------|-----------------------------------------|
    /// | `fsync`               | `off`   | Skip WAL flush to disk                  |
    /// | `synchronous_commit`  | `off`   | Don't wait for WAL write confirmation   |
    /// | `full_page_writes`    | `off`   | Skip full-page images after checkpoint  |
    /// | `autovacuum`          | `off`   | Disable background vacuum               |
    #[must_use]
    pub fn config_param(mut self, key: &str, value: &str) -> Self {
        self.server_configs.insert(key.to_owned(), value.to_owned());
        self
    }

    /// Adds an arbitrary argument to the `initdb` command.
    ///
    /// Does NOT automatically prefix with `--` if the key already starts with `-`.
    /// If the value is empty, only the key is passed. If a key is set multiple
    /// times, the last value wins.
    ///
    /// Only applies when initializing a fresh data directory. Ignored if the
    /// data directory already contains a database cluster.
    #[must_use]
    pub fn initdb_arg(mut self, key: &str, value: &str) -> Self {
        self.initdb_args.insert(key.to_owned(), value.to_owned());
        self
    }

    /// Sets the directory containing PostgreSQL binaries (`initdb`, `postgres`, etc.).
    ///
    /// When set, binaries are resolved by joining this path with the binary name
    /// instead of using PATH-based discovery. This enables explicit version selection
    /// when multiple PostgreSQL installations exist.
    #[must_use]
    pub fn bin_path(mut self, path: impl AsRef<Path>) -> Self {
        self.bin_path = Some(path.as_ref().to_path_buf());
        self
    }

    /// Controls whether [`Drop`] performs a blocking waitpid loop to let
    /// PostgreSQL clean up before the process is reaped.
    ///
    /// | `wait_for_exit` | `kill_on_drop` | Drop behavior |
    /// |---|---|---|
    /// | `true` **(default)** | `false` | SIGINT → waitpid polling → SIGKILL escalation |
    /// | `false` | `true` | No blocking wait; tokio sends SIGKILL when `Child` drops |
    ///
    /// # When to disable
    ///
    /// The blocking wait exists so PostgreSQL can release SysV shared memory
    /// segments during shutdown. If you configure
    /// `shared_memory_type=mmap` (which does not allocate SysV segments),
    /// you can safely set this to `false` to avoid the up-to-10-second
    /// blocking poll in the [`Drop`] implementation.
    ///
    /// # Interaction with async shutdown
    ///
    /// This setting only affects the **synchronous [`Drop`] safety net**.
    /// The async shutdown path ([`PostgresProcess::shutdown_with_signal`])
    /// always sends the requested signal and waits via tokio's async
    /// `child.wait()`, regardless of this setting.
    #[must_use]
    pub fn wait_for_exit(mut self, wait: bool) -> Self {
        self.wait_for_exit = wait;
        self
    }

    /// Sets the PostgreSQL shared memory implementation.
    ///
    /// Delegates to [`config_param("shared_memory_type", ...)`](Self::config_param).
    /// See [`SharedMemoryType`] for available options.
    #[must_use]
    pub fn shared_memory_type(self, ty: SharedMemoryType) -> Self {
        self.config_param("shared_memory_type", &ty.to_string())
    }

    /// Sets the PostgreSQL dynamic shared memory implementation.
    ///
    /// Delegates to [`config_param("dynamic_shared_memory_type", ...)`](Self::config_param).
    /// See [`DynamicSharedMemoryType`] for available options.
    #[must_use]
    pub fn dynamic_shared_memory_type(self, ty: DynamicSharedMemoryType) -> Self {
        self.config_param("dynamic_shared_memory_type", &ty.to_string())
    }

    /// Sets the `shared_buffers` PostgreSQL parameter.
    ///
    /// Delegates to [`config_param("shared_buffers", ...)`](Self::config_param).
    /// Accepts a [`SharedBuffers`] value constructed via typed constructors
    /// (e.g., `SharedBuffers::from_mb(128)`).
    #[must_use]
    pub fn shared_buffers(self, value: SharedBuffers) -> Self {
        self.config_param("shared_buffers", &value.to_string())
    }

    /// Builds the effective server configuration by merging dev-mode defaults with
    /// caller overrides. Caller values take precedence.
    fn effective_server_configs(&self) -> BTreeMap<String, String> {
        let mut configs = BTreeMap::from([
            ("fsync".into(), "off".into()),
            ("synchronous_commit".into(), "off".into()),
            ("full_page_writes".into(), "off".into()),
            ("autovacuum".into(), "off".into()),
        ]);
        // Caller overrides take precedence
        for (k, v) in &self.server_configs {
            configs.insert(k.clone(), v.clone());
        }
        configs
    }

    /// Resolves a PostgreSQL binary path, using `bin_path` if set or falling back
    /// to PATH-based discovery via the `which` crate.
    fn resolve_binary(&self, name: &str) -> Result<PathBuf, PostgresError> {
        if let Some(ref bin_dir) = self.bin_path {
            let path = bin_dir.join(name);
            if path.exists() {
                return Ok(path);
            }
            return Err(PostgresError::BinaryNotFound {
                name: name.to_string(),
            });
        }
        find_binary(name)
    }

    /// Starts the PostgreSQL server and returns a handle and background future.
    ///
    /// This method:
    /// 1. Ensures the data directory exists with correct permissions
    /// 2. Cleans up any orphan postgres processes
    /// 3. Runs `initdb` if the data directory is fresh (no `PG_VERSION`), passing
    ///    the configured locale, encoding, and any extra initdb args
    /// 4. Starts `postgres` with dev-mode defaults merged with any caller-provided
    ///    server config params
    /// 5. Waits for the server to become ready
    /// 6. Returns `(Handle, impl Future)` for service composition
    ///
    /// The returned future uses `tokio::select!` to monitor two events:
    /// - **Shutdown notification**: the [`Handle`] calls
    ///   [`graceful_shutdown()`](Handle::graceful_shutdown) or
    ///   [`force_shutdown()`](Handle::force_shutdown)
    /// - **Unexpected child exit**: the postgres process crashes or exits on its own
    ///
    /// Signal handling (SIGINT/SIGTERM) is NOT done here — the caller (e.g.
    /// `solo_cmd`) owns the signal handler and consumes the handle to trigger
    /// shutdown. On notification, the future sends the appropriate signal
    /// (SIGTERM for graceful, SIGINT for hard), waits with a timeout, and
    /// escalates to SIGKILL. On unexpected exit, it returns an error.
    pub async fn start(
        self,
    ) -> Result<(Handle, impl Future<Output = Result<(), PostgresError>>), PostgresError> {
        let process = self.start_process().await?;

        let url = process.connection_url().to_string();
        let data_dir = process.data_dir.clone();

        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();

        let handle = Handle::new(url, data_dir, shutdown_tx);

        let fut = async move {
            let mut process = process;

            // Wait for either a shutdown signal from the Handle or unexpected child exit.
            // Signal handling is NOT done here — the caller (e.g. solo_cmd) owns
            // the signal handler and calls graceful_shutdown()/hard_shutdown() on
            // the Handle, which sends a ShutdownSignal through the channel.
            let signal = tokio::select! {
                result = shutdown_rx => match result {
                    Ok(ShutdownSignal::Hard) => {
                        tracing::info!("Hard shutdown requested, sending SIGINT to PostgreSQL");
                        nix::sys::signal::Signal::SIGINT
                    }
                    Ok(ShutdownSignal::Graceful) => {
                        tracing::info!("Graceful shutdown requested, sending SIGTERM to PostgreSQL");
                        nix::sys::signal::Signal::SIGTERM
                    }
                    Err(_) => {
                        tracing::info!(
                            "Handle dropped without explicit shutdown, sending SIGTERM to PostgreSQL"
                        );
                        nix::sys::signal::Signal::SIGTERM
                    }
                },
                status = process.wait_child() => {
                    let exit_status =
                        status.map_err(|e| PostgresError::ShutdownFailed { source: e })?;
                    tracing::error!(
                        status = ?exit_status,
                        "PostgreSQL exited unexpectedly"
                    );
                    return Err(PostgresError::UnexpectedExit {
                        status: exit_status.code(),
                    });
                },
            };

            process.shutdown_with_signal(signal).await?;
            Ok(())
        };

        Ok((handle, fut))
    }

    /// Starts the PostgreSQL process using the builder's configuration.
    ///
    /// Returns a [`PostgresProcess`] directly, without wrapping in a service handle
    /// and background future. Used internally by [`start()`](Self::start) and
    /// [`PostgresProcess::start()`].
    pub(crate) async fn start_process(self) -> Result<PostgresProcess, PostgresError> {
        tracing::info!(
            data_dir = %self.data_dir.display(),
            "Starting PostgreSQL database service"
        );

        ensure_data_dir(&self.data_dir).await?;
        cleanup_orphan_process(&self.data_dir).await?;

        let is_fresh = !is_initialized(&self.data_dir).await;
        if is_fresh {
            self.run_initdb().await?;
        } else {
            tracing::info!(
                data_dir = %self.data_dir.display(),
                "Reusing existing PostgreSQL data directory"
            );
            if self.locale.is_some() || self.encoding.is_some() || !self.initdb_args.is_empty() {
                tracing::debug!(
                    "Skipping initdb args for existing data directory \
                     (locale, encoding, and initdb_args only apply on first init)"
                );
            }
        }

        log_postgres_version_with(&self).await;

        let mut child = self.start_postgres_server().await?;
        let connection_url = build_connection_url(&self.data_dir);

        // Spawn async tasks to forward subprocess output to tracing.
        // Tasks terminate automatically when the child exits (EOF on pipes).
        let stdout_log_task = child.stdout.take().map(|stdout| {
            tokio::spawn(async move {
                let reader = BufReader::new(stdout);
                let mut lines = reader.lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    tracing::info!(target: "postgres", "{}", line);
                }
            })
        });

        let stderr_log_task = child.stderr.take().map(|stderr| {
            tokio::spawn(async move {
                let reader = BufReader::new(stderr);
                let mut lines = reader.lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    tracing::warn!(target: "postgres", "{}", line);
                }
            })
        });

        let mut process = PostgresProcess {
            child,
            data_dir: self.data_dir,
            connection_url,
            stdout_log_task,
            stderr_log_task,
            wait_for_exit: self.wait_for_exit,
        };

        process.wait_for_ready().await?;

        tracing::info!(
            data_dir = %process.data_dir.display(),
            url = %process.connection_url,
            fresh_database = is_fresh,
            "PostgreSQL database ready"
        );

        Ok(process)
    }

    /// Initializes a new PostgreSQL data directory using [`initdb`].
    ///
    /// Runs the following command:
    ///
    /// ```text
    /// initdb -D <data_dir> [--locale=<locale>] [--encoding=<encoding>] --auth=trust [<extra_args>...]
    /// ```
    ///
    /// ## Arguments
    ///
    /// | Flag                    | Purpose                                             |
    /// |-------------------------|-----------------------------------------------------|
    /// | `-D <data_dir>`         | Data directory for the new cluster                  |
    /// | `--locale=<locale>`     | Locale for the cluster (e.g., `C` for deterministic sort) |
    /// | `--encoding=<encoding>` | Character encoding for all databases (e.g., `UTF8`) |
    /// | `--auth=trust`          | Trust authentication for local connections (no password) |
    ///
    /// Extra arguments from [`PostgresBuilder::initdb_arg`] are appended after
    /// `--auth=trust`. Locale and encoding are only included when set via the
    /// builder; the flags are omitted otherwise, letting `initdb` use its defaults.
    ///
    /// ## PostgreSQL Reference
    ///
    /// - [`initdb`](https://www.postgresql.org/docs/16/app-initdb.html) (PostgreSQL 16)
    /// - [Locale Support](https://www.postgresql.org/docs/16/locale.html)
    /// - [Character Set Support](https://www.postgresql.org/docs/16/multibyte.html)
    /// - [Trust Authentication](https://www.postgresql.org/docs/16/auth-trust.html)
    ///
    /// ## Version Notes
    ///
    /// Tested with PostgreSQL 16, 17, 18. The arguments used are stable across
    /// these versions.
    async fn run_initdb(&self) -> Result<(), PostgresError> {
        let initdb_path = self.resolve_binary("initdb")?;

        tracing::info!(
            data_dir = %self.data_dir.display(),
            initdb = %initdb_path.display(),
            "Initializing PostgreSQL data directory"
        );

        let mut cmd = Command::new(&initdb_path);
        cmd.arg("-D").arg(&self.data_dir);

        if let Some(ref locale) = self.locale {
            cmd.arg(format!("--locale={locale}"));
        }
        if let Some(ref encoding) = self.encoding {
            cmd.arg(format!("--encoding={encoding}"));
        }

        cmd.arg("--auth=trust");

        // Apply extra initdb args
        for (key, value) in &self.initdb_args {
            if key.starts_with('-') {
                cmd.arg(key);
            } else {
                cmd.arg(format!("--{key}"));
            }
            if !value.is_empty() {
                cmd.arg(value);
            }
        }

        let output = cmd
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .await
            .map_err(|err| PostgresError::InitDbFailed {
                data_dir: self.data_dir.clone(),
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
            data_dir = %self.data_dir.display(),
            "PostgreSQL data directory initialized"
        );

        Ok(())
    }

    /// Starts the PostgreSQL server process using [`postgres`].
    ///
    /// Runs the following command:
    ///
    /// ```text
    /// postgres -D <data_dir> -k <data_dir> -h "" [-c key=val ...]
    /// ```
    ///
    /// ## Arguments
    ///
    /// | Flag              | Purpose                                              |
    /// |-------------------|------------------------------------------------------|
    /// | `-D <data_dir>`   | Data directory containing the database cluster       |
    /// | `-k <data_dir>`   | Directory for the Unix-domain socket file            |
    /// | `-h ""`           | Disable TCP/IP listening (Unix socket only)          |
    /// | `-c key=val`      | Server runtime parameters (one per parameter)        |
    ///
    /// Dev-mode defaults (`fsync=off`, `synchronous_commit=off`,
    /// `full_page_writes=off`, `autovacuum=off`) are applied via `-c` unless
    /// overridden by [`PostgresBuilder::config_param`]. See
    /// [`effective_server_configs`](Self::effective_server_configs).
    ///
    /// Both stdout and stderr are piped for log forwarding via tracing.
    /// The `PostgresProcess::Drop` impl performs a full blocking shutdown
    /// sequence (SIGINT → waitpid polling → SIGKILL escalation) as the
    /// safety net for future cancellation.
    ///
    /// ## PostgreSQL Reference
    ///
    /// - [`postgres`](https://www.postgresql.org/docs/16/app-postgres.html) (PostgreSQL 16)
    /// - [Connection Settings (`unix_socket_directories`)](https://www.postgresql.org/docs/16/runtime-config-connection.html#GUC-UNIX-SOCKET-DIRECTORIES)
    /// - [Connection Settings (`listen_addresses`)](https://www.postgresql.org/docs/16/runtime-config-connection.html#GUC-LISTEN-ADDRESSES)
    /// - [Non-Durable Settings](https://www.postgresql.org/docs/16/non-durability.html)
    ///
    /// ## Version Notes
    ///
    /// Tested with PostgreSQL 16, 17, 18. The arguments used are stable across
    /// these versions.
    async fn start_postgres_server(&self) -> Result<Child, PostgresError> {
        let postgres_path = self.resolve_binary("postgres")?;

        tracing::info!(
            data_dir = %self.data_dir.display(),
            postgres = %postgres_path.display(),
            "Starting PostgreSQL server"
        );

        let mut cmd = Command::new(&postgres_path);
        cmd.arg("-D")
            .arg(&self.data_dir)
            .arg("-k")
            .arg(&self.data_dir)
            .arg("-h")
            .arg("");

        // Apply effective server configs as -c key=val
        let configs = self.effective_server_configs();
        for (key, value) in &configs {
            cmd.arg("-c").arg(format!("{key}={value}"));
        }

        // Pipe both stdout and stderr so we can forward to tracing
        let child = cmd
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(!self.wait_for_exit)
            .spawn()
            .map_err(|err| PostgresError::StartFailed { source: err })?;

        Ok(child)
    }
}

/// Logs the PostgreSQL server version via `postgres --version`.
///
/// Runs:
///
/// ```text
/// postgres --version
/// ```
///
/// | Flag        | Purpose                            |
/// |-------------|------------------------------------|
/// | `--version` | Print the postgres version and exit |
///
/// ## PostgreSQL Reference
///
/// - [`postgres`](https://www.postgresql.org/docs/16/app-postgres.html) (PostgreSQL 16)
async fn log_postgres_version_with(builder: &PostgresBuilder) {
    let postgres_path = match builder.resolve_binary("postgres") {
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

/// A running PostgreSQL server process.
///
/// This struct manages the lifecycle of a PostgreSQL server process. The server
/// is automatically shut down when this struct is dropped.
///
/// Use [`PostgresBuilder`] to create instances of this type.
pub(crate) struct PostgresProcess {
    /// The postgres server child process
    child: Child,
    /// Path to the data directory
    data_dir: PathBuf,
    /// Connection URL for this instance
    connection_url: String,
    /// Handle for the stdout log forwarding task
    stdout_log_task: Option<JoinHandle<()>>,
    /// Handle for the stderr log forwarding task
    stderr_log_task: Option<JoinHandle<()>>,
    /// Whether [`Drop`] should perform a blocking waitpid loop.
    /// See [`PostgresBuilder::wait_for_exit`].
    wait_for_exit: bool,
}

impl PostgresProcess {
    /// Returns the connection URL for this PostgreSQL instance
    ///
    /// The URL uses Unix socket connections in the format:
    /// `postgresql:///postgres?host=/path/to/data_dir`
    pub fn connection_url(&self) -> &str {
        &self.connection_url
    }

    /// Waits for the PostgreSQL server to become ready by probing the Unix socket.
    ///
    /// Attempts to connect to the postgres Unix socket using exponential backoff
    /// via the `backon` crate. A successful `UnixStream::connect()` proves the
    /// server is accepting connections. The entire probe is wrapped in
    /// `tokio::time::timeout()` to enforce an overall deadline.
    ///
    /// Before each connection attempt, checks whether the child process has exited
    /// unexpectedly (which would make further retries pointless).
    async fn wait_for_ready(&mut self) -> Result<(), PostgresError> {
        let socket_path = self.data_dir.join(SOCKET_FILENAME);
        let timeout = Duration::from_secs(DEFAULT_READINESS_TIMEOUT_SECS);
        let start = std::time::Instant::now();

        tracing::debug!(
            socket_path = %socket_path.display(),
            timeout_secs = DEFAULT_READINESS_TIMEOUT_SECS,
            "Waiting for PostgreSQL to accept connections"
        );

        // The retry closure must be a pure function (no &mut self capture), so we
        // pass the socket path by value and handle child process checks externally.
        let probe_result = tokio::time::timeout(timeout, async {
            let socket = socket_path.clone();

            let result = (|| {
                let socket = socket.clone();
                async move {
                    tokio::net::UnixStream::connect(&socket).await.map_err(|_| {
                        PostgresError::ReadinessTimeout {
                            timeout_secs: DEFAULT_READINESS_TIMEOUT_SECS,
                        }
                    })
                }
            })
            .retry(
                ExponentialBuilder::default()
                    .with_min_delay(Duration::from_millis(50))
                    .with_max_delay(Duration::from_secs(2))
                    .with_max_times(60),
            )
            .notify(|_err, dur| {
                tracing::trace!(
                    retry_after_ms = dur.as_millis() as u64,
                    "socket not ready, retrying"
                );
            })
            .await;

            // Drop the UnixStream — we only needed it to confirm connectivity
            result.map(|_stream| ())
        })
        .await;

        match probe_result {
            Ok(Ok(())) => {
                tracing::debug!(
                    elapsed_ms = start.elapsed().as_millis() as u64,
                    "PostgreSQL is accepting connections"
                );
                Ok(())
            }
            Ok(Err(err)) => {
                // Retries exhausted — check if child exited before reporting timeout
                if let Some(status) = self
                    .child
                    .try_wait()
                    .map_err(|e| PostgresError::StartFailed { source: e })?
                {
                    return Err(PostgresError::UnexpectedExit {
                        status: status.code(),
                    });
                }
                let _ = self.child.kill().await;
                // Reap the killed child to prevent zombie process
                let _ = self.child.wait().await;
                Err(err)
            }
            Err(_timeout) => {
                // Check if child exited before reporting timeout
                if let Some(status) = self
                    .child
                    .try_wait()
                    .map_err(|e| PostgresError::StartFailed { source: e })?
                {
                    return Err(PostgresError::UnexpectedExit {
                        status: status.code(),
                    });
                }
                let _ = self.child.kill().await;
                // Reap the killed child to prevent zombie process
                let _ = self.child.wait().await;
                Err(PostgresError::ReadinessTimeout {
                    timeout_secs: DEFAULT_READINESS_TIMEOUT_SECS,
                })
            }
        }
    }

    /// Waits for the PostgreSQL child process to exit.
    ///
    /// Returns the exit status when the process terminates. This is intended
    /// for use in a `tokio::select!` branch to detect unexpected exits while
    /// the service is running.
    pub(crate) async fn wait_child(&mut self) -> Result<std::process::ExitStatus, std::io::Error> {
        self.child.wait().await
    }

    /// Shuts down the PostgreSQL server with the specified signal.
    ///
    /// - `SIGTERM` — smart shutdown: waits for sessions to disconnect.
    /// - `SIGINT`  — fast shutdown: aborts transactions, disconnects clients.
    ///
    /// If the process does not exit within the timeout after the initial signal,
    /// escalates to SIGKILL and reaps the child to prevent zombies.
    pub(crate) async fn shutdown_with_signal(
        mut self,
        signal: nix::sys::signal::Signal,
    ) -> Result<(), PostgresError> {
        self.shutdown_inner(signal).await
    }

    async fn shutdown_inner(
        &mut self,
        signal: nix::sys::signal::Signal,
    ) -> Result<(), PostgresError> {
        tracing::info!(
            data_dir = %self.data_dir.display(),
            signal = %signal,
            "Shutting down PostgreSQL server"
        );

        // Send the requested signal via nix crate (safe Rust, no shell)
        #[cfg(unix)]
        if let Some(pid) = self.child.id() {
            // SAFETY: PID limits are well under `i32::MAX` (~2.1 billion) on all
            // supported platforms — Linux caps at 4_194_304, macOS at 99_999.
            let nix_pid = i32::try_from(pid)
                .map(nix::unistd::Pid::from_raw)
                .expect("PID exceeds i32::MAX");
            if let Err(err) = nix::sys::signal::kill(nix_pid, signal) {
                tracing::warn!(pid = pid, signal = %signal, error = %err, "Failed to send signal to PostgreSQL");
            }
        }

        // Wait for process to exit (with timeout)
        let timeout = Duration::from_secs(10);
        let result = tokio::time::timeout(timeout, self.child.wait()).await;

        // Abort log forwarding tasks — the process is exiting so pipes will EOF,
        // but abort ensures cleanup even if the reader is blocked
        self.abort_log_tasks();

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
                // Timeout — force kill via native tokio API (sends SIGKILL)
                tracing::warn!("PostgreSQL shutdown timed out, forcing kill");
                self.child
                    .kill()
                    .await
                    .map_err(|err| PostgresError::ShutdownFailed { source: err })?;
                // Reap the killed child to prevent zombie process
                let _ = self.child.wait().await;
                Ok(())
            }
        }
    }

    /// Aborts the stdout/stderr log forwarding tasks if they are still running.
    fn abort_log_tasks(&mut self) {
        if let Some(handle) = self.stdout_log_task.take() {
            handle.abort();
        }
        if let Some(handle) = self.stderr_log_task.take() {
            handle.abort();
        }
    }

    /// Blocking shutdown for the Drop safety net.
    ///
    /// Sends SIGINT (fast shutdown: aborts transactions, doesn't wait for
    /// sessions) then polls waitpid with WNOHANG to let PostgreSQL complete
    /// its SysV shared memory cleanup. On timeout, escalates to SIGKILL.
    ///
    /// Happy path: when the async shutdown already completed, child.id()
    /// returns None (tokio already reaped) and we return immediately.
    #[cfg(unix)]
    fn shutdown_blocking(&mut self) {
        use nix::sys::wait::WaitPidFlag;

        let Some(pid) = self.child.id() else {
            // Already reaped by the async shutdown path — nothing to do.
            return;
        };

        // SAFETY: PID limits are well under `i32::MAX` (~2.1 billion) on all
        // supported platforms — Linux caps at 4_194_304, macOS at 99_999.
        let nix_pid = i32::try_from(pid)
            .map(nix::unistd::Pid::from_raw)
            .expect("PID exceeds i32::MAX");

        // Fast shutdown — SIGINT aborts transactions and exits promptly,
        // unlike SIGTERM (smart shutdown) which waits for sessions to
        // disconnect voluntarily.
        if nix::sys::signal::kill(nix_pid, nix::sys::signal::Signal::SIGINT).is_err() {
            return; // process already gone
        }

        // Poll waitpid(WNOHANG): 100 iterations × 100 ms = 10 s timeout.
        for _ in 0..100 {
            match nix::sys::wait::waitpid(nix_pid, Some(WaitPidFlag::WNOHANG)) {
                Ok(nix::sys::wait::WaitStatus::StillAlive) => {
                    std::thread::sleep(Duration::from_millis(100));
                }
                // Process exited, signaled, or any other terminal state.
                Ok(_) => return,
                // ECHILD: not our child or already reaped — either way, done.
                Err(_) => return,
            }
        }

        // Timeout — escalate to SIGKILL.
        tracing::warn!(
            pid = pid,
            "PostgreSQL did not exit within 10 s after SIGINT in Drop, sending SIGKILL"
        );
        let _ = nix::sys::signal::kill(nix_pid, nix::sys::signal::Signal::SIGKILL);

        // Brief reap attempt: 5 iterations × 100 ms = 500 ms.
        for _ in 0..5 {
            match nix::sys::wait::waitpid(nix_pid, Some(WaitPidFlag::WNOHANG)) {
                Ok(nix::sys::wait::WaitStatus::StillAlive) => {
                    std::thread::sleep(std::time::Duration::from_millis(100));
                }
                _ => return,
            }
        }
        // Give up — tokio's orphan queue handles the rest.
    }
}

impl Drop for PostgresProcess {
    fn drop(&mut self) {
        self.abort_log_tasks();
        if self.wait_for_exit {
            self.shutdown_blocking();
        }
    }
}

/// Signal sent from [`Handle`] to the background future to initiate shutdown.
///
/// Decoupled from `nix::sys::signal::Signal` so the public API does not
/// depend on the `nix` crate.
#[derive(Debug, Clone, Copy)]
pub(crate) enum ShutdownSignal {
    /// Graceful (smart) shutdown — postgres waits for sessions to disconnect.
    Graceful,
    /// Hard (fast) shutdown — postgres aborts transactions and disconnects clients.
    Hard,
}

/// Handle to interact with the PostgreSQL database service.
///
/// The caller **must** consume this handle by calling either
/// [`graceful_shutdown()`](Handle::graceful_shutdown) or
/// [`force_shutdown()`](Handle::force_shutdown). Both methods consume `self` by
/// value, returning an inert [`ShuttingDown`] token. The background future
/// then performs the actual shutdown sequence (signal → timeout → SIGKILL).
///
/// Dropping the handle without calling a shutdown method is a programming
/// error — the background future will hang on the oneshot receiver until it is
/// cancelled (e.g., by aborting its `JoinHandle`). When the future is
/// cancelled, `PostgresProcess::Drop` performs a full blocking shutdown
/// sequence (SIGINT → waitpid polling → SIGKILL escalation) to ensure
/// PostgreSQL completes SysV shared memory cleanup.
#[derive(Debug)]
pub struct Handle {
    url: String,
    data_dir: PathBuf,
    shutdown_tx: tokio::sync::oneshot::Sender<ShutdownSignal>,
}

/// Inert token proving that shutdown was initiated.
///
/// Returned by [`Handle::graceful_shutdown`] and [`Handle::force_shutdown`].
/// Has no methods and no `Drop` side-effects — the background future handles
/// the actual shutdown sequence.
pub struct ShuttingDown(());

impl Handle {
    /// Creates a new handle (crate-internal).
    pub(crate) fn new(
        url: String,
        data_dir: PathBuf,
        shutdown_tx: tokio::sync::oneshot::Sender<ShutdownSignal>,
    ) -> Self {
        Self {
            url,
            data_dir,
            shutdown_tx,
        }
    }

    /// Gets the connection URL for connecting to this database.
    ///
    /// The URL uses Unix socket connections in the format:
    /// `postgresql:///postgres?host=/path/to/data_dir`
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Gets the data directory where PostgreSQL stores its files.
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// Initiates a graceful (smart) shutdown by sending SIGTERM to postgres.
    ///
    /// Postgres waits for all sessions to disconnect voluntarily, then exits.
    /// If the process does not exit within the timeout, the background future
    /// escalates to SIGKILL.
    ///
    /// Consumes the handle — the caller cannot interact with the service after
    /// initiating shutdown.
    pub fn graceful_shutdown(self) -> ShuttingDown {
        let _ignore_send_error = self.shutdown_tx.send(ShutdownSignal::Graceful);
        ShuttingDown(())
    }

    /// Initiates a hard (fast) shutdown by sending SIGINT to postgres.
    ///
    /// Postgres aborts active transactions, disconnects clients, then exits.
    /// If the process does not exit within the timeout, the background future
    /// escalates to SIGKILL.
    ///
    /// Consumes the handle — the caller cannot interact with the service after
    /// initiating shutdown.
    pub fn force_shutdown(self) -> ShuttingDown {
        let _ignore_send_error = self.shutdown_tx.send(ShutdownSignal::Hard);
        ShuttingDown(())
    }
}

/// Finds a PostgreSQL binary in PATH using the `which` crate.
///
/// Resolves binaries via the system PATH, providing cross-platform binary
/// discovery with clear error messages when binaries are not found.
fn find_binary(name: &str) -> Result<PathBuf, PostgresError> {
    which::which(name).map_err(|_| PostgresError::BinaryNotFound {
        name: name.to_string(),
    })
}

/// Ensures the data directory exists with proper permissions
async fn ensure_data_dir(data_dir: &Path) -> Result<(), PostgresError> {
    let dir_exists = fs_err::tokio::metadata(data_dir)
        .await
        .is_ok_and(|m| m.is_dir());
    if !dir_exists {
        fs_err::tokio::create_dir_all(data_dir)
            .await
            .map_err(|err| PostgresError::CreateDataDir { source: err })?;
    }

    // Set directory permissions to 700 (required by PostgreSQL)
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let permissions = std::fs::Permissions::from_mode(0o700);
        fs_err::tokio::set_permissions(data_dir, permissions)
            .await
            .map_err(|err| PostgresError::SetPermissions { source: err })?;
    }

    Ok(())
}

/// Checks if the data directory has been initialized (PG_VERSION exists)
async fn is_initialized(data_dir: &Path) -> bool {
    fs_err::tokio::metadata(data_dir.join("PG_VERSION"))
        .await
        .is_ok_and(|m| m.is_file())
}

/// Detects and cleans up orphan PostgreSQL processes from previous runs.
///
/// Reads the [`postmaster.pid`] file to find a leftover PID, checks whether
/// that process is still alive (signal 0 via [`nix`]), and terminates it
/// (SIGTERM → wait → SIGKILL) before cleaning up stale files.
///
/// ## `postmaster.pid` Format
///
/// PostgreSQL writes a `postmaster.pid` file to the data directory on
/// startup. The first line contains the server's PID as a decimal integer.
/// Subsequent lines contain additional metadata (data directory, start
/// timestamp, port, socket directory, etc.) but only the PID is used here.
///
/// ## PostgreSQL Reference
///
/// - [Database File Layout (`postmaster.pid`)](https://www.postgresql.org/docs/16/storage-file-layout.html) (PostgreSQL 16)
async fn cleanup_orphan_process(data_dir: &Path) -> Result<(), PostgresError> {
    let pid_path = data_dir.join(POSTMASTER_PID_FILE);
    let socket_path = data_dir.join(SOCKET_FILENAME);

    let pid_file_exists = fs_err::tokio::metadata(&pid_path)
        .await
        .is_ok_and(|m| m.is_file());
    if !pid_file_exists {
        return Ok(());
    }

    // Read the PID from postmaster.pid (first line)
    let pid_content = match fs_err::tokio::read_to_string(&pid_path).await {
        Ok(content) => content,
        Err(err) => {
            tracing::debug!(
                error = %err,
                "Failed to read postmaster.pid, attempting cleanup"
            );
            // Try to remove stale files anyway
            let _ = fs_err::tokio::remove_file(&pid_path).await;
            let _ = fs_err::tokio::remove_file(&socket_path).await;
            return Ok(());
        }
    };

    let Some(pid_str) = pid_content.lines().next() else {
        tracing::debug!("Empty postmaster.pid file, cleaning up");
        let _ = fs_err::tokio::remove_file(&pid_path).await;
        let _ = fs_err::tokio::remove_file(&socket_path).await;
        return Ok(());
    };

    let pid: u32 = match pid_str.trim().parse() {
        Ok(pid) => pid,
        Err(_) => {
            tracing::debug!(
                pid_str = pid_str,
                "Invalid PID in postmaster.pid, cleaning up stale files"
            );
            let _ = fs_err::tokio::remove_file(&pid_path).await;
            let _ = fs_err::tokio::remove_file(&socket_path).await;
            return Ok(());
        }
    };

    // PID 0 is the kernel's process group alias — kill(0, sig) sends the signal
    // to every process in the caller's process group, which would terminate ampd
    // and potentially other co-grouped processes. Treat as a corrupt PID file.
    if pid == 0 {
        tracing::warn!("postmaster.pid contains PID 0 (invalid), cleaning up stale files");
        let _ = fs_err::tokio::remove_file(&pid_path).await;
        let _ = fs_err::tokio::remove_file(&socket_path).await;
        return Ok(());
    }

    // Check if process is still running using signal 0 (existence check).
    //
    // NOTE: This does not verify the process is actually PostgreSQL. If the
    // original postgres crashed and the OS recycled the PID, we could
    // terminate an unrelated process. This is the same inherent limitation
    // as `pg_ctl` and other PID-file-based approaches; the race window is
    // small in practice.
    // SAFETY: PID limits are well under `i32::MAX` (~2.1 billion) on all
    // supported platforms — Linux caps at 4_194_304, macOS at 99_999.
    let nix_pid = i32::try_from(pid)
        .map(nix::unistd::Pid::from_raw)
        .expect("PID exceeds i32::MAX");
    let process_exists = nix::sys::signal::kill(nix_pid, None).is_ok();

    if process_exists {
        tracing::warn!(
            pid = pid,
            data_dir = %data_dir.display(),
            "Detected orphan PostgreSQL process, terminating"
        );

        // Send SIGTERM to orphan process
        let _ = nix::sys::signal::kill(nix_pid, nix::sys::signal::Signal::SIGTERM);

        // Wait for orphan process to exit after SIGTERM, using backon retry
        let exited = (|| async {
            match nix::sys::signal::kill(nix_pid, None) {
                Err(_) => Ok(()), // Process gone — success
                Ok(()) => Err("process still running"),
            }
        })
        .retry(
            ExponentialBuilder::default()
                .with_min_delay(Duration::from_millis(100))
                .with_max_delay(Duration::from_secs(1))
                .with_max_times(50),
        )
        .await;

        if exited.is_ok() {
            tracing::info!(pid = pid, "Orphan PostgreSQL process terminated");
        } else {
            // Force kill if still running after retry budget
            tracing::warn!(pid = pid, "Orphan process not responding, force killing");
            let _ = nix::sys::signal::kill(nix_pid, nix::sys::signal::Signal::SIGKILL);

            // Brief wait for SIGKILL to take effect, using backon retry
            let _ = (|| async {
                match nix::sys::signal::kill(nix_pid, None) {
                    Err(_) => Ok(()),
                    Ok(()) => Err("process still running after SIGKILL"),
                }
            })
            .retry(
                ExponentialBuilder::default()
                    .with_min_delay(Duration::from_millis(50))
                    .with_max_delay(Duration::from_millis(200))
                    .with_max_times(10),
            )
            .await;
        }
    } else {
        tracing::debug!(
            pid = pid,
            "Stale postmaster.pid found (process not running), cleaning up"
        );
    }

    // Clean up stale files
    let _ = fs_err::tokio::remove_file(&pid_path).await;
    let _ = fs_err::tokio::remove_file(&socket_path).await;

    Ok(())
}

/// Builds a PostgreSQL connection URL using the Unix socket in `data_dir`.
///
/// Produces a [connection URI] of the form:
///
/// ```text
/// postgresql:///postgres?host=<data_dir>
/// ```
///
/// The empty host before the first `/` means "use Unix socket", and the
/// `host` query parameter points to the directory containing the
/// `.s.PGSQL.5432` socket file (the data directory in our case).
///
/// ## PostgreSQL Reference
///
/// - [Connection Strings](https://www.postgresql.org/docs/16/libpq-connect.html#LIBPQ-CONNSTRING) (PostgreSQL 16)
///
/// [connection URI]: https://www.postgresql.org/docs/16/libpq-connect.html#LIBPQ-CONNSTRING
/// Characters that must be percent-encoded in URI query parameter values.
///
/// Encodes control characters plus URI-sensitive characters (space, `"`, `#`,
/// `%`, `&`, `+`, `=`, `?`) while leaving path separators (`/`) and other
/// common path characters unencoded for log readability.
const QUERY_VALUE_ENCODE_SET: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'"')
    .add(b'#')
    .add(b'%')
    .add(b'&')
    .add(b'+')
    .add(b'=')
    .add(b'?');

fn build_connection_url(data_dir: &Path) -> String {
    let host_path = data_dir.to_string_lossy();
    let encoded = utf8_percent_encode(&host_path, QUERY_VALUE_ENCODE_SET);
    format!("postgresql:///postgres?host={encoded}")
}

/// Errors that can occur during PostgreSQL process management
///
/// This enum covers all failure modes when managing a PostgreSQL instance,
/// from finding binaries to starting and stopping the database.
#[derive(Debug, thiserror::Error)]
pub enum PostgresError {
    /// PostgreSQL binary not found in PATH
    ///
    /// This error occurs when a required PostgreSQL binary (`initdb`, `postgres`,
    /// cannot be found in the system PATH.
    ///
    /// Common causes:
    /// - PostgreSQL is not installed on the system
    /// - PostgreSQL bin directory is not in PATH
    /// - Wrong PostgreSQL version installed
    #[error("PostgreSQL binary '{name}' not found in PATH")]
    BinaryNotFound {
        /// Name of the binary that was not found
        name: String,
    },

    /// Failed to initialize the PostgreSQL data directory
    ///
    /// This error occurs when `initdb` fails to create a new database cluster.
    ///
    /// Common causes:
    /// - Insufficient permissions to create the data directory
    /// - Data directory already exists and is not empty
    /// - Disk space issues
    #[error("Failed to initialize PostgreSQL data directory at '{data_dir}'")]
    InitDbFailed {
        /// Path to the data directory that failed to initialize
        data_dir: PathBuf,
        /// The underlying IO or process error
        #[source]
        source: std::io::Error,
    },

    /// initdb process exited with non-zero status
    ///
    /// This error occurs when `initdb` runs but exits with an error code.
    #[error("initdb exited with status {status}: {stderr}")]
    InitDbExitError {
        /// Exit status code
        status: i32,
        /// Standard error output from initdb
        stderr: String,
    },

    /// Failed to start the PostgreSQL server
    ///
    /// This error occurs when the `postgres` process cannot be spawned.
    ///
    /// Common causes:
    /// - Insufficient permissions
    /// - Data directory does not exist or is corrupted
    /// - Another PostgreSQL instance is using the same data directory
    #[error("Failed to start PostgreSQL server")]
    StartFailed {
        /// The underlying IO error
        #[source]
        source: std::io::Error,
    },

    /// PostgreSQL server failed to become ready
    ///
    /// This error occurs when the PostgreSQL server starts but doesn't
    /// create its socket file within the expected timeout.
    #[error("PostgreSQL server failed to become ready within {timeout_secs} seconds")]
    ReadinessTimeout {
        /// Number of seconds waited before timing out
        timeout_secs: u64,
    },

    /// Failed to create the data directory
    ///
    /// This error occurs when the parent directories for the data path
    /// cannot be created. The underlying `fs_err` error includes the path
    /// in its message automatically.
    #[error("Failed to create data directory")]
    CreateDataDir {
        /// The underlying IO error (includes path context via `fs_err`)
        #[source]
        source: std::io::Error,
    },

    /// Failed to set permissions on the data directory
    ///
    /// PostgreSQL requires the data directory to have restricted permissions (700).
    /// The underlying `fs_err` error includes the path in its message automatically.
    #[error("Failed to set permissions on data directory")]
    SetPermissions {
        /// The underlying IO error (includes path context via `fs_err`)
        #[source]
        source: std::io::Error,
    },

    /// Failed to shut down the PostgreSQL server
    ///
    /// This error occurs when the server cannot be stopped gracefully.
    #[error("Failed to shut down PostgreSQL server")]
    ShutdownFailed {
        /// The underlying IO error
        #[source]
        source: std::io::Error,
    },

    /// PostgreSQL server exited unexpectedly
    ///
    /// This error occurs when the postgres process terminates while still expected
    /// to be running.
    #[error("PostgreSQL server exited unexpectedly with status {status:?}")]
    UnexpectedExit {
        /// Exit status code, if available
        status: Option<i32>,
    },
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
        assert_eq!(url, "postgresql:///postgres?host=/tmp/test%20db");
    }

    #[test]
    fn builder_effective_server_configs_applies_dev_mode_defaults() {
        let builder = PostgresBuilder::new("/tmp/test");
        let configs = builder.effective_server_configs();

        assert_eq!(configs.get("fsync").expect("fsync should be set"), "off");
        assert_eq!(
            configs
                .get("synchronous_commit")
                .expect("synchronous_commit should be set"),
            "off"
        );
        assert_eq!(
            configs
                .get("full_page_writes")
                .expect("full_page_writes should be set"),
            "off"
        );
        assert_eq!(
            configs.get("autovacuum").expect("autovacuum should be set"),
            "off"
        );
    }

    #[test]
    fn builder_effective_server_configs_allows_caller_overrides() {
        let builder = PostgresBuilder::new("/tmp/test")
            .config_param("fsync", "on")
            .config_param("max_connections", "50");
        let configs = builder.effective_server_configs();

        // Caller override wins
        assert_eq!(configs.get("fsync").expect("fsync should be set"), "on");
        // Dev-mode defaults still present for non-overridden keys
        assert_eq!(
            configs
                .get("synchronous_commit")
                .expect("synchronous_commit should be set"),
            "off"
        );
        // Caller-added keys present
        assert_eq!(
            configs
                .get("max_connections")
                .expect("max_connections should be set"),
            "50"
        );
    }

    #[test]
    fn builder_methods_are_chainable() {
        let builder = PostgresBuilder::new("/tmp/test")
            .locale("C")
            .encoding("UTF8")
            .config_param("fsync", "on")
            .shared_memory_type(SharedMemoryType::SysV)
            .dynamic_shared_memory_type(DynamicSharedMemoryType::Mmap)
            .shared_buffers(SharedBuffers::from_mb(256))
            .initdb_arg("--data-checksums", "")
            .bin_path("/usr/lib/postgresql/18/bin");

        assert_eq!(builder.locale.as_deref(), Some("C"));
        assert_eq!(builder.encoding.as_deref(), Some("UTF8"));
        assert_eq!(
            builder.server_configs.get("fsync").map(String::as_str),
            Some("on")
        );
        assert_eq!(
            builder
                .server_configs
                .get("shared_memory_type")
                .map(String::as_str),
            Some("sysvipc")
        );
        assert_eq!(
            builder
                .server_configs
                .get("dynamic_shared_memory_type")
                .map(String::as_str),
            Some("mmap")
        );
        assert_eq!(
            builder
                .server_configs
                .get("shared_buffers")
                .map(String::as_str),
            Some("256MB")
        );
        assert_eq!(
            builder
                .initdb_args
                .get("--data-checksums")
                .map(String::as_str),
            Some("")
        );
        assert_eq!(
            builder.bin_path,
            Some(PathBuf::from("/usr/lib/postgresql/18/bin"))
        );
    }

    #[test]
    fn shared_memory_type_display() {
        assert_eq!(SharedMemoryType::Mmap.to_string(), "mmap");
        assert_eq!(SharedMemoryType::SysV.to_string(), "sysvipc");
    }

    #[test]
    fn dynamic_shared_memory_type_display() {
        assert_eq!(DynamicSharedMemoryType::Posix.to_string(), "posix");
        assert_eq!(DynamicSharedMemoryType::SysV.to_string(), "sysv");
        assert_eq!(DynamicSharedMemoryType::Mmap.to_string(), "mmap");
    }

    #[test]
    fn shared_buffers_from_constructors() {
        assert_eq!(SharedBuffers::from_kb(1).0.as_u64(), 1024);
        assert_eq!(SharedBuffers::from_mb(1).0.as_u64(), 1024 * 1024);
        assert_eq!(SharedBuffers::from_gb(1).0.as_u64(), 1024 * 1024 * 1024);
    }

    #[test]
    fn shared_buffers_display() {
        assert_eq!(SharedBuffers::from_gb(1).to_string(), "1GB");
        assert_eq!(SharedBuffers::from_gb(4).to_string(), "4GB");
        assert_eq!(SharedBuffers::from_mb(128).to_string(), "128MB");
        assert_eq!(SharedBuffers::from_mb(256).to_string(), "256MB");
        assert_eq!(SharedBuffers::from_kb(256).to_string(), "256kB");
        assert_eq!(SharedBuffers::from_kb(1024).to_string(), "1MB");
        assert_eq!(SharedBuffers::from_mb(1024).to_string(), "1GB");
    }
}
