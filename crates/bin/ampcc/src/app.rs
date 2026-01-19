//! Application state and business logic.

use std::{
    collections::HashMap,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use admin_client::{
    Client,
    jobs::JobInfo,
    workers::{WorkerDetailResponse, WorkerInfo},
};
use anyhow::{Context, Result};
use ratatui::widgets::ScrollbarState;
use url::Url;

use crate::{config::Config, registry::RegistryClient};

/// Cursor position in multi-line text.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TextPosition {
    pub line: usize,
    pub column: usize,
}

/// The history file structure for persistence.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct HistoryFile {
    pub dataset_history: HashMap<String, Vec<String>>,
}

/// The favorites file structure for persistence.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FavoritesFile {
    pub version: u32,
    pub favorites: Vec<String>,
}

/// A SQL query template.
#[derive(Debug, Clone)]
pub struct QueryTemplate {
    /// The template pattern with placeholders.
    pub pattern: &'static str,
    /// Short description of the template.
    pub description: &'static str,
}

/// Available query templates.
pub const QUERY_TEMPLATES: &[QueryTemplate] = &[
    QueryTemplate {
        pattern: "SELECT * FROM {table} LIMIT 10",
        description: "Preview data",
    },
    QueryTemplate {
        pattern: "SELECT COUNT(*) FROM {table}",
        description: "Row count",
    },
    QueryTemplate {
        pattern: "SELECT * FROM {table} WHERE {column} = '?'",
        description: "Filter by column",
    },
    QueryTemplate {
        pattern: "SELECT {column}, COUNT(*) FROM {table} GROUP BY {column}",
        description: "Group by",
    },
    QueryTemplate {
        pattern: "SELECT DISTINCT {column} FROM {table}",
        description: "Unique values",
    },
    QueryTemplate {
        pattern: "DESCRIBE {table}",
        description: "Table schema",
    },
];

/// Input mode for the application.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputMode {
    Normal,
    Search,
    Query,
}

/// Active pane for focus tracking.
/// In Local mode: Header -> Datasets -> Jobs -> Workers -> Detail -> Header
/// In Registry mode: Header -> Datasets -> Manifest -> Schema -> Header
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActivePane {
    Header,
    Datasets,
    Jobs,        // Local only
    Workers,     // Local only
    Manifest,    // Dataset manifest pane (content area)
    Schema,      // Dataset schema pane (content area)
    Detail,      // Job/Worker detail view (Local only)
    Query,       // SQL query input pane (Local only)
    QueryResult, // SQL query results pane (Local only)
}

impl ActivePane {
    /// Cycle to the next pane.
    /// Local mode: Header -> Datasets -> Jobs -> Workers -> Detail -> Header
    /// Registry mode: Header -> Datasets -> Manifest -> Schema -> Header
    /// Query panes: Query -> QueryResult -> Datasets (exit query mode)
    pub fn next(self, is_local: bool) -> Self {
        match self {
            ActivePane::Header => ActivePane::Datasets,
            ActivePane::Datasets => {
                if is_local {
                    ActivePane::Jobs
                } else {
                    ActivePane::Manifest
                }
            }
            ActivePane::Jobs => ActivePane::Workers,
            ActivePane::Workers => ActivePane::Manifest,
            ActivePane::Manifest => ActivePane::Schema,
            ActivePane::Schema => ActivePane::Detail,
            ActivePane::Detail => ActivePane::Header,
            // Query panes cycle within query view
            ActivePane::Query => ActivePane::QueryResult,
            ActivePane::QueryResult => ActivePane::Datasets,
        }
    }

    /// Cycle to the previous pane.
    /// Local mode: Header -> Detail -> Workers -> Jobs -> Datasets -> Header
    /// Registry mode: Header -> Schema -> Manifest -> Datasets -> Header
    /// Query panes: QueryResult -> Query -> Datasets (exit query mode)
    pub fn prev(self, is_local: bool) -> Self {
        match self {
            ActivePane::Header => {
                if is_local {
                    ActivePane::Detail
                } else {
                    ActivePane::Schema
                }
            }
            ActivePane::Datasets => ActivePane::Header,
            ActivePane::Jobs => ActivePane::Datasets,
            ActivePane::Workers => ActivePane::Jobs,
            ActivePane::Manifest => ActivePane::Datasets,
            ActivePane::Schema => ActivePane::Manifest,
            ActivePane::Detail => ActivePane::Workers,
            // Query panes cycle within query view
            ActivePane::Query => ActivePane::Datasets,
            ActivePane::QueryResult => ActivePane::Query,
        }
    }
}

/// What content is displayed in the detail pane.
#[derive(Debug, Clone)]
pub enum ContentView {
    /// Dataset manifest and schema (existing behavior)
    Dataset,
    /// Job details
    Job(JobInfo),
    /// Worker details
    Worker(WorkerDetailResponse),
    /// SQL query results
    QueryResults,
    /// Nothing selected
    None,
}

/// Query results from SQL execution.
#[derive(Debug, Clone, Default)]
pub struct QueryResults {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<String>>,
    pub row_count: usize,
    pub error: Option<String>,
}

/// Data source for datasets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataSource {
    Local,
    Registry,
}

impl DataSource {
    pub fn as_str(&self) -> &'static str {
        match self {
            DataSource::Local => "Local",
            DataSource::Registry => "Registry",
        }
    }
}

/// A version entry for a dataset.
#[derive(Debug, Clone)]
pub struct VersionEntry {
    pub version_tag: String,
    pub status: String,
    #[allow(dead_code)]
    pub created_at: String,
    pub is_latest: bool,
}

/// A unified dataset entry that works for both local and registry sources.
#[derive(Debug, Clone)]
pub struct DatasetEntry {
    pub namespace: String,
    pub name: String,
    pub latest_version: Option<String>,
    pub description: Option<String>,
    pub versions: Option<Vec<VersionEntry>>,
    pub expanded: bool,
}

impl DatasetEntry {
    /// Get the display label for the dataset.
    #[allow(dead_code)]
    pub fn label(&self) -> String {
        if let Some(version) = &self.latest_version {
            format!("{}/{} @{}", self.namespace, self.name, version)
        } else {
            format!("{}/{}", self.namespace, self.name)
        }
    }
}

/// Selection type - either a dataset or a version within an expanded dataset.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum SelectedItem {
    Dataset {
        index: usize,
        namespace: String,
        name: String,
        version: Option<String>,
    },
    Version {
        dataset_index: usize,
        version_index: usize,
        namespace: String,
        name: String,
        version: String,
    },
}

/// Column information for inspect view.
#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub arrow_type: String,
    pub nullable: bool,
}

/// Table schema for inspect view.
#[derive(Debug, Clone)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<ColumnInfo>,
}

/// Result of inspecting a dataset manifest.
#[derive(Debug, Clone)]
pub struct InspectResult {
    pub tables: Vec<TableSchema>,
}

impl InspectResult {
    /// Parse a manifest JSON to extract schema information.
    pub fn from_manifest(manifest: &serde_json::Value) -> Option<Self> {
        let tables_obj = manifest.get("tables")?.as_object()?;
        let mut tables = Vec::new();

        for (table_name, table_def) in tables_obj {
            let schema = table_def.get("schema")?;
            let arrow = schema.get("arrow")?;
            let fields = arrow.get("fields")?.as_array()?;

            let columns: Vec<ColumnInfo> = fields
                .iter()
                .filter_map(|field| {
                    let name = field.get("name")?.as_str()?.to_string();
                    let arrow_type = format_arrow_type(field.get("type")?);
                    let nullable = field
                        .get("nullable")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(true);
                    Some(ColumnInfo {
                        name,
                        arrow_type,
                        nullable,
                    })
                })
                .collect();

            tables.push(TableSchema {
                name: table_name.clone(),
                columns,
            });
        }

        // Sort tables by name for consistent display
        tables.sort_by(|a, b| a.name.cmp(&b.name));

        Some(InspectResult { tables })
    }
}

/// Format an Arrow type from the manifest JSON.
fn format_arrow_type(type_value: &serde_json::Value) -> String {
    if let Some(type_str) = type_value.as_str() {
        return type_str.to_string();
    }

    if let Some(obj) = type_value.as_object() {
        // Handle complex types like Timestamp, FixedSizeBinary, etc.
        if let Some((type_name, type_params)) = obj.iter().next() {
            return format_complex_type(type_name, type_params);
        }
    }

    "Unknown".to_string()
}

/// Format a complex Arrow type given its name and parameters.
fn format_complex_type(type_name: &str, params: &serde_json::Value) -> String {
    match type_name {
        "FixedSizeBinary" => {
            let size = params.as_u64().unwrap_or(0);
            format!("FixedSizeBinary({})", size)
        }
        "Decimal128" | "Decimal256" => {
            if let Some(obj) = params.as_object() {
                let precision = obj.get("precision").and_then(|v| v.as_u64()).unwrap_or(0);
                let scale = obj.get("scale").and_then(|v| v.as_i64()).unwrap_or(0);
                format!("{}({}, {})", type_name, precision, scale)
            } else {
                type_name.to_string()
            }
        }
        "Timestamp" => {
            if let Some(arr) = params.as_array() {
                let unit = arr.first().and_then(|v| v.as_str()).unwrap_or("unknown");
                let tz = arr
                    .get(1)
                    .and_then(|v| v.as_str())
                    .filter(|s| !s.is_empty())
                    .map(|s| format!(", {}", s))
                    .unwrap_or_default();
                format!("Timestamp({}{})", unit, tz)
            } else {
                "Timestamp".to_string()
            }
        }
        "Duration" => {
            if let Some(unit) = params.as_str() {
                format!("Duration({})", unit)
            } else if let Some(obj) = params.as_object() {
                let unit = obj
                    .get("unit")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown");
                format!("Duration({})", unit)
            } else {
                "Duration".to_string()
            }
        }
        "Time32" | "Time64" => {
            if let Some(unit) = params.as_str() {
                format!("{}({})", type_name, unit)
            } else {
                type_name.to_string()
            }
        }
        "FixedSizeList" => {
            if let Some(arr) = params.as_array() {
                let size = arr.get(1).and_then(|v| v.as_u64()).unwrap_or(0);
                let child = arr
                    .first()
                    .and_then(|c| c.get("data_type"))
                    .map(format_arrow_type)
                    .unwrap_or_else(|| "?".to_string());
                format!("FixedSizeList({}, {})", size, child)
            } else {
                "FixedSizeList".to_string()
            }
        }
        "List" | "LargeList" => {
            if let Some(obj) = params.as_object() {
                let child = obj
                    .get("data_type")
                    .map(format_arrow_type)
                    .unwrap_or_else(|| "?".to_string());
                format!("{}({})", type_name, child)
            } else {
                type_name.to_string()
            }
        }
        "Interval" => {
            if let Some(unit) = params.as_str() {
                format!("Interval({})", unit)
            } else {
                "Interval".to_string()
            }
        }
        "Dictionary" => {
            if let Some(arr) = params.as_array() {
                let key_type = arr.first().and_then(|v| v.as_str()).unwrap_or("unknown");
                let val_type = arr.get(1).and_then(|v| v.as_str()).unwrap_or("unknown");
                format!("Dictionary({}, {})", key_type, val_type)
            } else {
                "Dictionary".to_string()
            }
        }
        _ => "Unknown".to_string(),
    }
}

/// Main application state.
pub struct App {
    pub config: Config,

    // Clients
    pub local_client: Arc<Client>,
    pub registry_client: RegistryClient,

    // Data source
    pub current_source: DataSource,

    // UI state
    pub should_quit: bool,
    pub input_mode: InputMode,
    pub active_pane: ActivePane,
    pub search_query: String,

    // Dataset state
    pub datasets: Vec<DatasetEntry>,
    pub filtered_datasets: Vec<DatasetEntry>,
    pub selected_index: usize,

    // For expanded datasets, track which version is selected
    // Key: dataset index in filtered_datasets, Value: selected version index
    pub selected_version_indices: HashMap<usize, usize>,

    // Jobs state (Local mode only)
    pub jobs: Vec<JobInfo>,
    pub selected_job_index: usize,

    // Workers state (Local mode only)
    pub workers: Vec<WorkerInfo>,
    pub selected_worker_index: usize,

    // Content view state - what's shown in the detail pane
    pub content_view: ContentView,

    // Auto-refresh timer for jobs/workers
    pub last_refresh: Instant,

    // Manifest state (for Dataset content view)
    pub current_manifest: Option<serde_json::Value>,
    pub current_inspect: Option<InspectResult>,

    // Loading state
    pub loading: bool,
    pub error_message: Option<String>,

    // Success message (auto-expires after 3 seconds)
    pub success_message: Option<String>,
    pub message_expires: Option<Instant>,

    // Spinner animation state
    pub spinner_frame: usize,
    pub loading_message: Option<String>,

    // Scroll state for content panes
    pub manifest_scroll: u16,
    pub manifest_scroll_state: ScrollbarState,
    pub schema_scroll: u16,
    pub schema_scroll_state: ScrollbarState,
    pub detail_scroll: u16,
    pub detail_scroll_state: ScrollbarState,

    // Content length for scroll bounds
    pub manifest_content_length: usize,
    pub schema_content_length: usize,
    pub detail_content_length: usize,

    // Redraw flag for CPU optimization
    pub needs_redraw: bool,

    // SQL Query state (Local mode only)
    pub query_input: String,
    pub query_cursor: TextPosition,
    pub query_input_scroll: u16, // Scroll offset for tall query input
    pub query_results: Option<QueryResults>,
    pub query_scroll: u16,
    pub query_scroll_state: ScrollbarState,
    pub query_content_length: usize,

    // Query history (per-dataset, persisted)
    pub query_history: HashMap<String, Vec<String>>,
    pub query_history_index: Option<usize>, // None = current input, Some(i) = history[i]
    pub query_draft: String,                // Preserved current input when navigating history

    // History search state (Ctrl+R)
    pub history_search_active: bool,
    pub history_search_query: String,
    pub history_search_matches: Vec<usize>, // indices into query_history
    pub history_search_index: usize,        // index into matches

    // Template picker state
    pub template_picker_open: bool,
    pub template_picker_index: usize,

    // Result sorting state
    pub result_sort_column: Option<usize>,
    pub result_sort_ascending: bool,
    pub result_sort_pending: bool, // true when waiting for column number input

    // Favorite queries state
    pub favorite_queries: Vec<String>,
    pub favorites_panel_open: bool,
    pub favorites_panel_index: usize,
}

impl App {
    /// Create a new application instance.
    pub fn new(config: Config) -> Result<Self> {
        let admin_url = Url::parse(&config.local_admin_url).context("invalid admin URL")?;
        let local_client = Arc::new(Client::new(admin_url));

        let registry_client = RegistryClient::new(config.registry_url.clone());

        let default_source = if config.default_source == "local" {
            DataSource::Local
        } else {
            DataSource::Registry
        };

        Ok(Self {
            config,
            local_client,
            registry_client,
            current_source: default_source,
            should_quit: false,
            input_mode: InputMode::Normal,
            active_pane: ActivePane::Datasets,
            search_query: String::new(),
            datasets: Vec::new(),
            filtered_datasets: Vec::new(),
            selected_index: 0,
            selected_version_indices: HashMap::new(),
            jobs: Vec::new(),
            selected_job_index: 0,
            workers: Vec::new(),
            selected_worker_index: 0,
            content_view: ContentView::None,
            last_refresh: Instant::now(),
            current_manifest: None,
            current_inspect: None,
            loading: false,
            error_message: None,
            success_message: None,
            message_expires: None,
            spinner_frame: 0,
            loading_message: None,
            manifest_scroll: 0,
            manifest_scroll_state: ScrollbarState::default(),
            schema_scroll: 0,
            schema_scroll_state: ScrollbarState::default(),
            detail_scroll: 0,
            detail_scroll_state: ScrollbarState::default(),
            manifest_content_length: 0,
            schema_content_length: 0,
            detail_content_length: 0,
            needs_redraw: true,
            query_input: String::new(),
            query_cursor: TextPosition::default(),
            query_input_scroll: 0,
            query_results: None,
            query_scroll: 0,
            query_scroll_state: ScrollbarState::default(),
            query_content_length: 0,
            query_history: HashMap::new(),
            query_history_index: None,
            query_draft: String::new(),
            history_search_active: false,
            history_search_query: String::new(),
            history_search_matches: Vec::new(),
            history_search_index: 0,
            template_picker_open: false,
            template_picker_index: 0,
            result_sort_column: None,
            result_sort_ascending: true,
            result_sort_pending: false,
            favorite_queries: Vec::new(),
            favorites_panel_open: false,
            favorites_panel_index: 0,
        })
    }

    /// Spinner frames for loading animation (braille pattern).
    pub const SPINNER_FRAMES: &'static [char] = &['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'];

    /// Quit the application.
    pub fn quit(&mut self) {
        self.should_quit = true;
    }

    /// Advance the spinner animation frame.
    pub fn tick_spinner(&mut self) {
        if self.loading {
            self.spinner_frame = (self.spinner_frame + 1) % Self::SPINNER_FRAMES.len();
        }
    }

    /// Get the current spinner character.
    pub fn spinner_char(&self) -> char {
        Self::SPINNER_FRAMES[self.spinner_frame % Self::SPINNER_FRAMES.len()]
    }

    /// Start loading with a message.
    pub fn start_loading(&mut self, message: &str) {
        self.loading = true;
        self.loading_message = Some(message.to_string());
        self.error_message = None;
    }

    /// Stop loading.
    pub fn stop_loading(&mut self) {
        self.loading = false;
        self.loading_message = None;
    }

    /// Set a success message that auto-expires after 3 seconds.
    pub fn set_success_message(&mut self, msg: String) {
        self.success_message = Some(msg);
        self.message_expires = Some(Instant::now() + Duration::from_secs(3));
        // Clear any error message when showing success
        self.error_message = None;
    }

    /// Tick message expiration - call in main loop.
    pub fn tick_messages(&mut self) {
        if let Some(expires) = self.message_expires
            && Instant::now() > expires
        {
            self.success_message = None;
            self.message_expires = None;
        }
    }

    /// Scroll up in the focused pane.
    pub fn scroll_up(&mut self) {
        match self.active_pane {
            ActivePane::Manifest => {
                self.manifest_scroll = self.manifest_scroll.saturating_sub(1);
                self.manifest_scroll_state = self
                    .manifest_scroll_state
                    .position(self.manifest_scroll as usize);
            }
            ActivePane::Schema => {
                self.schema_scroll = self.schema_scroll.saturating_sub(1);
                self.schema_scroll_state = self
                    .schema_scroll_state
                    .position(self.schema_scroll as usize);
            }
            ActivePane::Detail => {
                self.detail_scroll = self.detail_scroll.saturating_sub(1);
                self.detail_scroll_state = self
                    .detail_scroll_state
                    .position(self.detail_scroll as usize);
            }
            ActivePane::QueryResult => {
                self.query_scroll = self.query_scroll.saturating_sub(1);
                self.query_scroll_state =
                    self.query_scroll_state.position(self.query_scroll as usize);
            }
            ActivePane::Header
            | ActivePane::Datasets
            | ActivePane::Jobs
            | ActivePane::Workers
            | ActivePane::Query => {}
        }
    }

    /// Scroll down in the focused pane.
    pub fn scroll_down(&mut self) {
        match self.active_pane {
            ActivePane::Manifest => {
                let max_scroll = self.manifest_content_length.saturating_sub(1);
                if (self.manifest_scroll as usize) < max_scroll {
                    self.manifest_scroll = self.manifest_scroll.saturating_add(1);
                    self.manifest_scroll_state = self
                        .manifest_scroll_state
                        .position(self.manifest_scroll as usize);
                }
            }
            ActivePane::Schema => {
                let max_scroll = self.schema_content_length.saturating_sub(1);
                if (self.schema_scroll as usize) < max_scroll {
                    self.schema_scroll = self.schema_scroll.saturating_add(1);
                    self.schema_scroll_state = self
                        .schema_scroll_state
                        .position(self.schema_scroll as usize);
                }
            }
            ActivePane::Detail => {
                let max_scroll = self.detail_content_length.saturating_sub(1);
                if (self.detail_scroll as usize) < max_scroll {
                    self.detail_scroll = self.detail_scroll.saturating_add(1);
                    self.detail_scroll_state = self
                        .detail_scroll_state
                        .position(self.detail_scroll as usize);
                }
            }
            ActivePane::QueryResult => {
                let max_scroll = self.query_content_length.saturating_sub(1);
                if (self.query_scroll as usize) < max_scroll {
                    self.query_scroll = self.query_scroll.saturating_add(1);
                    self.query_scroll_state =
                        self.query_scroll_state.position(self.query_scroll as usize);
                }
            }
            ActivePane::Header
            | ActivePane::Datasets
            | ActivePane::Jobs
            | ActivePane::Workers
            | ActivePane::Query => {}
        }
    }

    /// Reset scroll positions when content changes.
    pub fn reset_scroll(&mut self) {
        self.manifest_scroll = 0;
        self.manifest_scroll_state = ScrollbarState::default();
        self.schema_scroll = 0;
        self.schema_scroll_state = ScrollbarState::default();
        self.detail_scroll = 0;
        self.detail_scroll_state = ScrollbarState::default();
        self.query_scroll = 0;
        self.query_scroll_state = ScrollbarState::default();
        self.manifest_content_length = 0;
        self.schema_content_length = 0;
        self.detail_content_length = 0;
        self.query_content_length = 0;
    }

    /// Page up in the focused pane.
    pub fn page_up(&mut self, page_size: u16) {
        match self.active_pane {
            ActivePane::Manifest => {
                self.manifest_scroll = self.manifest_scroll.saturating_sub(page_size);
                self.manifest_scroll_state = self
                    .manifest_scroll_state
                    .position(self.manifest_scroll as usize);
            }
            ActivePane::Schema => {
                self.schema_scroll = self.schema_scroll.saturating_sub(page_size);
                self.schema_scroll_state = self
                    .schema_scroll_state
                    .position(self.schema_scroll as usize);
            }
            ActivePane::Detail => {
                self.detail_scroll = self.detail_scroll.saturating_sub(page_size);
                self.detail_scroll_state = self
                    .detail_scroll_state
                    .position(self.detail_scroll as usize);
            }
            ActivePane::QueryResult => {
                self.query_scroll = self.query_scroll.saturating_sub(page_size);
                self.query_scroll_state =
                    self.query_scroll_state.position(self.query_scroll as usize);
            }
            ActivePane::Header
            | ActivePane::Datasets
            | ActivePane::Jobs
            | ActivePane::Workers
            | ActivePane::Query => {}
        }
    }

    /// Page down in the focused pane.
    pub fn page_down(&mut self, page_size: u16) {
        match self.active_pane {
            ActivePane::Manifest => {
                let max_scroll = self.manifest_content_length.saturating_sub(1) as u16;
                self.manifest_scroll = self
                    .manifest_scroll
                    .saturating_add(page_size)
                    .min(max_scroll);
                self.manifest_scroll_state = self
                    .manifest_scroll_state
                    .position(self.manifest_scroll as usize);
            }
            ActivePane::Schema => {
                let max_scroll = self.schema_content_length.saturating_sub(1) as u16;
                self.schema_scroll = self.schema_scroll.saturating_add(page_size).min(max_scroll);
                self.schema_scroll_state = self
                    .schema_scroll_state
                    .position(self.schema_scroll as usize);
            }
            ActivePane::Detail => {
                let max_scroll = self.detail_content_length.saturating_sub(1) as u16;
                self.detail_scroll = self.detail_scroll.saturating_add(page_size).min(max_scroll);
                self.detail_scroll_state = self
                    .detail_scroll_state
                    .position(self.detail_scroll as usize);
            }
            ActivePane::QueryResult => {
                let max_scroll = self.query_content_length.saturating_sub(1) as u16;
                self.query_scroll = self.query_scroll.saturating_add(page_size).min(max_scroll);
                self.query_scroll_state =
                    self.query_scroll_state.position(self.query_scroll as usize);
            }
            ActivePane::Header
            | ActivePane::Datasets
            | ActivePane::Jobs
            | ActivePane::Workers
            | ActivePane::Query => {}
        }
    }

    /// Get the current source URL for display.
    pub fn current_source_url(&self) -> &str {
        match self.current_source {
            DataSource::Local => &self.config.local_admin_url,
            DataSource::Registry => &self.config.registry_url,
        }
    }

    /// Fetch datasets from the current source.
    pub async fn fetch_datasets(&mut self) -> Result<()> {
        self.start_loading("Fetching datasets...");

        let result = match self.current_source {
            DataSource::Local => self.fetch_local_datasets().await,
            DataSource::Registry => self.fetch_registry_datasets().await,
        };

        self.stop_loading();

        match result {
            Ok(datasets) => {
                self.datasets = datasets;
                self.update_search();
                Ok(())
            }
            Err(e) => {
                self.error_message = Some(e.to_string());
                Err(e)
            }
        }
    }

    /// Fetch datasets from local admin API.
    async fn fetch_local_datasets(&self) -> Result<Vec<DatasetEntry>> {
        let response = self.local_client.datasets().list_all().await?;
        Ok(response
            .datasets
            .into_iter()
            .map(|d| DatasetEntry {
                namespace: d.namespace.to_string(),
                name: d.name.to_string(),
                latest_version: d.latest_version.map(|v| v.to_string()),
                description: None,
                versions: None,
                expanded: false,
            })
            .collect())
    }

    /// Fetch datasets from the registry.
    async fn fetch_registry_datasets(&self) -> Result<Vec<DatasetEntry>> {
        let mut all_datasets = Vec::new();
        let mut page = 1;

        loop {
            let response = self.registry_client.list_datasets(page).await?;
            for d in response.datasets {
                all_datasets.push(DatasetEntry {
                    namespace: d.namespace,
                    name: d.name,
                    latest_version: d.latest_version.and_then(|v| v.version_tag),
                    description: d.description,
                    versions: None,
                    expanded: false,
                });
            }

            if !response.has_next_page {
                break;
            }
            page += 1;
        }

        Ok(all_datasets)
    }

    /// Switch to a different data source.
    pub async fn switch_source(&mut self, source: DataSource) -> Result<()> {
        if self.current_source == source {
            return Ok(());
        }

        self.current_source = source;
        self.search_query.clear();
        self.selected_index = 0;
        self.selected_version_indices.clear();
        self.current_manifest = None;

        // Clear jobs/workers when switching away from Local
        if source == DataSource::Registry {
            self.jobs.clear();
            self.workers.clear();
            self.selected_job_index = 0;
            self.selected_worker_index = 0;
            self.content_view = ContentView::None;
            // If currently on Jobs or Workers pane, switch to Datasets
            if matches!(self.active_pane, ActivePane::Jobs | ActivePane::Workers) {
                self.active_pane = ActivePane::Datasets;
            }
        }

        self.fetch_datasets().await
    }

    /// Check if the current source is Local.
    pub fn is_local(&self) -> bool {
        self.current_source == DataSource::Local
    }

    /// Select the next job.
    pub fn select_next_job(&mut self) {
        if !self.jobs.is_empty() {
            self.selected_job_index = (self.selected_job_index + 1) % self.jobs.len();
        }
    }

    /// Select the previous job.
    pub fn select_previous_job(&mut self) {
        if !self.jobs.is_empty() {
            if self.selected_job_index == 0 {
                self.selected_job_index = self.jobs.len() - 1;
            } else {
                self.selected_job_index -= 1;
            }
        }
    }

    /// Select the next worker.
    pub fn select_next_worker(&mut self) {
        if !self.workers.is_empty() {
            self.selected_worker_index = (self.selected_worker_index + 1) % self.workers.len();
        }
    }

    /// Select the previous worker.
    pub fn select_previous_worker(&mut self) {
        if !self.workers.is_empty() {
            if self.selected_worker_index == 0 {
                self.selected_worker_index = self.workers.len() - 1;
            } else {
                self.selected_worker_index -= 1;
            }
        }
    }

    /// Get the currently selected job.
    pub fn get_selected_job(&self) -> Option<&JobInfo> {
        self.jobs.get(self.selected_job_index)
    }

    /// Get the currently selected worker.
    pub fn get_selected_worker(&self) -> Option<&WorkerInfo> {
        self.workers.get(self.selected_worker_index)
    }

    /// Check if a job status is terminal (completed, stopped, failed).
    pub fn is_job_terminal(status: &str) -> bool {
        matches!(
            status.to_lowercase().as_str(),
            "completed" | "stopped" | "failed" | "error"
        )
    }

    /// Check if a job can be stopped (not terminal).
    pub fn can_stop_job(status: &str) -> bool {
        !Self::is_job_terminal(status)
    }

    /// Update the filtered datasets based on search query.
    pub fn update_search(&mut self) {
        if self.search_query.is_empty() {
            self.filtered_datasets = self.datasets.clone();
        } else {
            let query = self.search_query.to_lowercase();
            self.filtered_datasets = self
                .datasets
                .iter()
                .filter(|d| {
                    d.namespace.to_lowercase().contains(&query)
                        || d.name.to_lowercase().contains(&query)
                        || d.description
                            .as_ref()
                            .map(|desc| desc.to_lowercase().contains(&query))
                            .unwrap_or(false)
                })
                .cloned()
                .collect();
        }

        // Reset selection if out of bounds
        if self.selected_index >= self.total_items() {
            self.selected_index = 0;
        }
        self.current_manifest = None;
    }

    /// Get the total number of selectable items (datasets + expanded versions).
    pub fn total_items(&self) -> usize {
        let mut count = 0;
        for (i, dataset) in self.filtered_datasets.iter().enumerate() {
            count += 1; // The dataset itself
            if dataset.expanded
                && let Some(versions) = &dataset.versions
            {
                count += versions.len();
            }
            // Update version indices if needed
            if !self.selected_version_indices.contains_key(&i) && dataset.expanded {
                // Will be set when needed
            }
        }
        count
    }

    /// Convert a flat index to (dataset_index, version_index).
    /// version_index is None if the dataset itself is selected.
    fn index_to_position(&self, flat_index: usize) -> Option<(usize, Option<usize>)> {
        let mut current = 0;
        for (dataset_idx, dataset) in self.filtered_datasets.iter().enumerate() {
            if current == flat_index {
                return Some((dataset_idx, None));
            }
            current += 1;

            if dataset.expanded
                && let Some(versions) = &dataset.versions
            {
                for version_idx in 0..versions.len() {
                    if current == flat_index {
                        return Some((dataset_idx, Some(version_idx)));
                    }
                    current += 1;
                }
            }
        }
        None
    }

    /// Convert (dataset_index, version_index) to flat index.
    fn position_to_index(&self, dataset_idx: usize, version_idx: Option<usize>) -> usize {
        let mut index = 0;
        for (i, dataset) in self.filtered_datasets.iter().enumerate() {
            if i == dataset_idx {
                if let Some(v_idx) = version_idx {
                    return index + 1 + v_idx;
                }
                return index;
            }
            index += 1;
            if dataset.expanded
                && let Some(versions) = &dataset.versions
            {
                index += versions.len();
            }
        }
        index
    }

    /// Select the next item.
    pub fn select_next(&mut self) {
        let total = self.total_items();
        if total > 0 {
            self.selected_index = (self.selected_index + 1) % total;
            self.current_manifest = None;
        }
    }

    /// Select the previous item.
    pub fn select_previous(&mut self) {
        let total = self.total_items();
        if total > 0 {
            if self.selected_index == 0 {
                self.selected_index = total - 1;
            } else {
                self.selected_index -= 1;
            }
            self.current_manifest = None;
        }
    }

    /// Get the currently selected item.
    pub fn get_selected_item(&self) -> Option<SelectedItem> {
        let (dataset_idx, version_idx) = self.index_to_position(self.selected_index)?;
        let dataset = self.filtered_datasets.get(dataset_idx)?;

        if let Some(v_idx) = version_idx {
            let version = dataset.versions.as_ref()?.get(v_idx)?;
            Some(SelectedItem::Version {
                dataset_index: dataset_idx,
                version_index: v_idx,
                namespace: dataset.namespace.clone(),
                name: dataset.name.clone(),
                version: version.version_tag.clone(),
            })
        } else {
            Some(SelectedItem::Dataset {
                index: dataset_idx,
                namespace: dataset.namespace.clone(),
                name: dataset.name.clone(),
                version: dataset.latest_version.clone(),
            })
        }
    }

    /// Get namespace, name, and version for the currently selected item (for manifest fetching).
    pub fn get_selected_manifest_params(&self) -> Option<(String, String, String)> {
        match self.get_selected_item()? {
            SelectedItem::Dataset {
                namespace,
                name,
                version,
                ..
            } => {
                let v = version?;
                Some((namespace, name, v))
            }
            SelectedItem::Version {
                namespace,
                name,
                version,
                ..
            } => Some((namespace, name, version)),
        }
    }

    /// Toggle expansion of the currently selected dataset.
    pub async fn toggle_expand(&mut self) -> Result<()> {
        let Some((dataset_idx, version_idx)) = self.index_to_position(self.selected_index) else {
            return Ok(());
        };

        // Only toggle if we're on a dataset, not a version
        if version_idx.is_some() {
            return Ok(());
        }

        let dataset = match self.filtered_datasets.get_mut(dataset_idx) {
            Some(d) => d,
            None => return Ok(()),
        };

        if dataset.expanded {
            // Collapse
            dataset.expanded = false;
            // Recalculate selected_index to stay on this dataset
            self.selected_index = self.position_to_index(dataset_idx, None);
        } else {
            // Expand - fetch versions if not already loaded
            let needs_fetch = dataset.versions.is_none();
            let namespace = dataset.namespace.clone();
            let name = dataset.name.clone();

            if needs_fetch {
                let versions = self.fetch_versions(&namespace, &name).await?;
                let dataset = self.filtered_datasets.get_mut(dataset_idx).unwrap();
                dataset.versions = Some(versions);
            }
            let dataset = self.filtered_datasets.get_mut(dataset_idx).unwrap();
            dataset.expanded = true;
        }

        Ok(())
    }

    /// Fetch versions for a dataset.
    async fn fetch_versions(&self, namespace: &str, name: &str) -> Result<Vec<VersionEntry>> {
        match self.current_source {
            DataSource::Local => {
                use datasets_common::fqn::FullyQualifiedName;

                // Construct FQN for the dataset
                let fqn_str = format!("{}/{}", namespace, name);
                let fqn: FullyQualifiedName = fqn_str
                    .parse()
                    .map_err(|e| anyhow::anyhow!("invalid FQN: {}", e))?;

                // Call admin API to list versions
                match self.local_client.datasets().list_versions(&fqn).await {
                    Ok(versions_response) => {
                        // Determine which version is the latest
                        let latest_version = versions_response.special_tags.latest.as_ref();

                        // Map API response to VersionEntry structs
                        let versions = versions_response
                            .versions
                            .into_iter()
                            .map(|v| {
                                let is_latest =
                                    latest_version.map(|lv| lv == &v.version).unwrap_or(false);
                                VersionEntry {
                                    version_tag: v.version.to_string(),
                                    status: "registered".to_string(),
                                    created_at: v.created_at,
                                    is_latest,
                                }
                            })
                            .collect();
                        Ok(versions)
                    }
                    Err(e) => {
                        // Log error but don't crash - graceful degradation
                        eprintln!("Failed to fetch versions for {}: {}", fqn_str, e);
                        Ok(Vec::new())
                    }
                }
            }
            DataSource::Registry => {
                let versions = self.registry_client.get_versions(namespace, name).await?;
                Ok(versions
                    .into_iter()
                    .enumerate()
                    .map(|(i, v)| VersionEntry {
                        version_tag: v.version_tag,
                        status: v.status,
                        created_at: v.created_at,
                        is_latest: i == 0,
                    })
                    .collect())
            }
        }
    }

    /// Fetch manifest for a specific dataset version.
    #[allow(dead_code)]
    pub async fn fetch_manifest(
        &self,
        namespace: &str,
        name: &str,
        version: &str,
    ) -> Result<Option<serde_json::Value>> {
        match self.current_source {
            DataSource::Local => {
                use datasets_common::{reference::Reference, revision::Revision};
                let revision: Revision = version
                    .parse()
                    .map_err(|e| anyhow::anyhow!("invalid revision: {}", e))?;
                let reference = Reference::new(namespace.parse()?, name.parse()?, revision);
                let manifest = self
                    .local_client
                    .datasets()
                    .get_manifest(&reference)
                    .await?;
                Ok(manifest)
            }
            DataSource::Registry => {
                let manifest = self
                    .registry_client
                    .get_manifest(namespace, name, version)
                    .await?;
                Ok(Some(manifest))
            }
        }
    }

    /// Get the path to the history file.
    pub fn history_file_path() -> Result<PathBuf> {
        let config_dir = directories::ProjectDirs::from("com", "thegraph", "ampcc")
            .context("could not determine config directory")?
            .config_dir()
            .to_path_buf();
        Ok(config_dir.join("history.json"))
    }

    /// Load history from disk on startup.
    pub fn load_history(&mut self) -> Result<()> {
        let path = Self::history_file_path()?;
        if !path.exists() {
            return Ok(()); // No history yet
        }

        let content = std::fs::read_to_string(&path)?;
        let file: HistoryFile = serde_json::from_str(&content)?;
        self.query_history = file.dataset_history;

        Ok(())
    }

    /// Save history to disk on shutdown.
    pub fn save_history(&self) -> Result<()> {
        let path = Self::history_file_path()?;

        // Ensure directory exists
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        // Limit history per dataset
        const MAX_HISTORY_PER_DATASET: usize = 100;
        let mut limited_history: HashMap<String, Vec<String>> = HashMap::new();
        for (key, queries) in &self.query_history {
            let limited: Vec<String> = queries
                .iter()
                .rev()
                .take(MAX_HISTORY_PER_DATASET)
                .rev()
                .cloned()
                .collect();
            if !limited.is_empty() {
                limited_history.insert(key.clone(), limited);
            }
        }

        let file = HistoryFile {
            dataset_history: limited_history,
        };

        let content = serde_json::to_string_pretty(&file)?;
        std::fs::write(&path, content)?;

        Ok(())
    }

    /// Get the history key for the current dataset.
    pub fn current_history_key(&self) -> String {
        if let Some((ns, name, _)) = self.get_selected_manifest_params() {
            format!("{}.{}", ns, name)
        } else {
            "global".to_string()
        }
    }

    /// Get the history for the current dataset.
    pub fn current_history(&self) -> Vec<String> {
        let key = self.current_history_key();
        self.query_history.get(&key).cloned().unwrap_or_default()
    }

    /// Add a query to the current dataset's history.
    pub fn add_to_history(&mut self, query: String) {
        let key = self.current_history_key();
        let history = self.query_history.entry(key).or_default();
        // Avoid consecutive duplicates
        if history.last() != Some(&query) {
            history.push(query);
            // Limit in-memory history
            const MAX_HISTORY: usize = 100;
            if history.len() > MAX_HISTORY {
                history.remove(0);
            }
        }
    }

    // ========================================================================
    // Multi-line Query Input Helpers
    // ========================================================================

    /// Get the lines of query input.
    pub fn query_lines(&self) -> Vec<&str> {
        if self.query_input.is_empty() {
            vec![""]
        } else {
            self.query_input.split('\n').collect()
        }
    }

    /// Get the number of lines in query input.
    pub fn query_line_count(&self) -> usize {
        if self.query_input.is_empty() {
            1
        } else {
            self.query_input.split('\n').count()
        }
    }

    /// Get the length of a specific line.
    pub fn get_line_length(&self, line: usize) -> usize {
        self.query_lines().get(line).map(|l| l.len()).unwrap_or(0)
    }

    /// Convert TextPosition to byte offset in query_input.
    pub fn cursor_to_offset(&self) -> usize {
        let mut offset = 0;
        for (i, line) in self.query_input.split('\n').enumerate() {
            if i == self.query_cursor.line {
                return offset + self.query_cursor.column.min(line.len());
            }
            offset += line.len() + 1; // +1 for newline
        }
        self.query_input.len()
    }

    /// Convert byte offset to TextPosition.
    pub fn offset_to_cursor(input: &str, offset: usize) -> TextPosition {
        let mut line = 0;
        let mut column = 0;
        let mut current = 0;

        for ch in input.chars() {
            if current >= offset {
                break;
            }
            if ch == '\n' {
                line += 1;
                column = 0;
            } else {
                column += 1;
            }
            current += ch.len_utf8();
        }

        TextPosition { line, column }
    }

    /// Move cursor up one line (returns true if moved).
    pub fn cursor_up(&mut self) -> bool {
        if self.query_cursor.line > 0 {
            self.query_cursor.line -= 1;
            let line_len = self.get_line_length(self.query_cursor.line);
            self.query_cursor.column = self.query_cursor.column.min(line_len);
            true
        } else {
            false
        }
    }

    /// Move cursor down one line (returns true if moved).
    pub fn cursor_down(&mut self) -> bool {
        let line_count = self.query_line_count();
        if self.query_cursor.line < line_count.saturating_sub(1) {
            self.query_cursor.line += 1;
            let line_len = self.get_line_length(self.query_cursor.line);
            self.query_cursor.column = self.query_cursor.column.min(line_len);
            true
        } else {
            false
        }
    }

    /// Move cursor left, wrapping to previous line if at start.
    pub fn cursor_left(&mut self) {
        if self.query_cursor.column > 0 {
            self.query_cursor.column -= 1;
        } else if self.query_cursor.line > 0 {
            // Move to end of previous line
            self.query_cursor.line -= 1;
            self.query_cursor.column = self.get_line_length(self.query_cursor.line);
        }
    }

    /// Move cursor right, wrapping to next line if at end.
    pub fn cursor_right(&mut self) {
        let line_len = self.get_line_length(self.query_cursor.line);
        if self.query_cursor.column < line_len {
            self.query_cursor.column += 1;
        } else if self.query_cursor.line < self.query_line_count().saturating_sub(1) {
            // Move to start of next line
            self.query_cursor.line += 1;
            self.query_cursor.column = 0;
        }
    }

    /// Move cursor to start of current line.
    pub fn cursor_home(&mut self) {
        self.query_cursor.column = 0;
    }

    /// Move cursor to end of current line.
    pub fn cursor_end(&mut self) {
        self.query_cursor.column = self.get_line_length(self.query_cursor.line);
    }

    /// Insert a character at cursor position.
    pub fn insert_char(&mut self, c: char) {
        let offset = self.cursor_to_offset();
        self.query_input.insert(offset, c);
        if c == '\n' {
            self.query_cursor.line += 1;
            self.query_cursor.column = 0;
        } else {
            self.query_cursor.column += 1;
        }
    }

    /// Delete character before cursor (backspace).
    /// Returns true if something was deleted.
    pub fn backspace(&mut self) -> bool {
        if self.query_cursor.column > 0 {
            // Delete character in current line
            let offset = self.cursor_to_offset();
            self.query_input.remove(offset - 1);
            self.query_cursor.column -= 1;
            true
        } else if self.query_cursor.line > 0 {
            // Join with previous line
            let offset = self.cursor_to_offset();
            let prev_line_len = self.get_line_length(self.query_cursor.line - 1);
            self.query_input.remove(offset - 1); // Remove the newline
            self.query_cursor.line -= 1;
            self.query_cursor.column = prev_line_len;
            true
        } else {
            false
        }
    }

    /// Delete character at cursor position (delete key).
    /// Returns true if something was deleted.
    pub fn delete_char(&mut self) -> bool {
        let offset = self.cursor_to_offset();
        if offset < self.query_input.len() {
            self.query_input.remove(offset);
            true
        } else {
            false
        }
    }

    /// Set query input and reset cursor to end.
    pub fn set_query_input(&mut self, input: String) {
        self.query_input = input;
        // Move cursor to end of input
        let line_count = self.query_line_count();
        if line_count > 0 {
            self.query_cursor.line = line_count - 1;
            self.query_cursor.column = self.get_line_length(self.query_cursor.line);
        } else {
            self.query_cursor = TextPosition::default();
        }
    }

    // ========================================================================
    // History Search (Ctrl+R)
    // ========================================================================

    /// Update history search matches based on current search query.
    pub fn update_history_search(&mut self) {
        let history = self.current_history();
        if self.history_search_query.is_empty() {
            // Empty search query matches all history
            self.history_search_matches = (0..history.len()).rev().collect();
        } else {
            let query_lower = self.history_search_query.to_lowercase();
            self.history_search_matches = history
                .iter()
                .enumerate()
                .filter(|(_, h)| h.to_lowercase().contains(&query_lower))
                .map(|(i, _)| i)
                .rev() // Most recent first
                .collect();
        }

        // Reset to first match
        self.history_search_index = 0;

        // Update query input to show current match
        if let Some(&idx) = self.history_search_matches.first()
            && let Some(query) = history.get(idx)
        {
            self.set_query_input(query.clone());
        }
    }

    /// Cycle to the next history search match.
    pub fn cycle_history_search(&mut self) {
        if self.history_search_matches.is_empty() {
            return;
        }

        self.history_search_index =
            (self.history_search_index + 1) % self.history_search_matches.len();

        let history = self.current_history();
        let idx = self.history_search_matches[self.history_search_index];
        if let Some(query) = history.get(idx) {
            self.set_query_input(query.clone());
        }
    }

    /// Enter history search mode.
    pub fn enter_history_search(&mut self) {
        // Save current input as draft before starting search
        if !self.history_search_active {
            self.query_draft = self.query_input.clone();
        }
        self.history_search_active = true;
        self.history_search_query.clear();
        self.history_search_matches.clear();
        self.history_search_index = 0;
    }

    /// Exit history search mode, accepting current match.
    pub fn accept_history_search(&mut self) {
        self.history_search_active = false;
        self.history_search_query.clear();
        // query_input already has the selected match
    }

    /// Cancel history search mode, restoring original input.
    pub fn cancel_history_search(&mut self) {
        self.history_search_active = false;
        self.history_search_query.clear();
        self.set_query_input(self.query_draft.clone());
    }

    /// Resolve template placeholders with current context.
    pub fn resolve_template(&self, template: &str) -> String {
        let table = self
            .current_inspect
            .as_ref()
            .and_then(|i| i.tables.first())
            .map(|t| t.name.as_str())
            .unwrap_or("table_name");

        let column = self
            .current_inspect
            .as_ref()
            .and_then(|i| i.tables.first())
            .and_then(|t| t.columns.first())
            .map(|c| c.name.as_str())
            .unwrap_or("column_name");

        template
            .replace("{table}", table)
            .replace("{column}", column)
    }

    /// Get sorted indices for query results based on current sort state.
    /// Returns None if no sort is active, otherwise returns sorted row indices.
    pub fn get_sorted_indices(&self) -> Option<Vec<usize>> {
        let col = self.result_sort_column?;
        let results = self.query_results.as_ref()?;

        if col >= results.columns.len() {
            return None;
        }

        let mut indices: Vec<usize> = (0..results.rows.len()).collect();
        let ascending = self.result_sort_ascending;

        indices.sort_by(|&a, &b| {
            let val_a = results.rows[a].get(col).map(|s| s.as_str()).unwrap_or("");
            let val_b = results.rows[b].get(col).map(|s| s.as_str()).unwrap_or("");

            // Try numeric comparison first
            let cmp = match (val_a.parse::<f64>(), val_b.parse::<f64>()) {
                (Ok(num_a), Ok(num_b)) => num_a
                    .partial_cmp(&num_b)
                    .unwrap_or(std::cmp::Ordering::Equal),
                _ => val_a.cmp(val_b),
            };

            if ascending { cmp } else { cmp.reverse() }
        });

        Some(indices)
    }

    /// Toggle sort on the specified column.
    /// If already sorted by this column, reverses direction.
    /// If sorted by different column, starts ascending sort on new column.
    pub fn toggle_sort(&mut self, col: usize) {
        if self.result_sort_column == Some(col) {
            // Toggle direction
            self.result_sort_ascending = !self.result_sort_ascending;
        } else {
            // New column, start ascending
            self.result_sort_column = Some(col);
            self.result_sort_ascending = true;
        }
        self.result_sort_pending = false;
    }

    /// Clear all sorting.
    pub fn clear_sort(&mut self) {
        self.result_sort_column = None;
        self.result_sort_ascending = true;
        self.result_sort_pending = false;
    }

    /// Get the path to the favorites file.
    pub fn favorites_file_path() -> Result<PathBuf> {
        let config_dir = directories::ProjectDirs::from("com", "thegraph", "ampcc")
            .context("could not determine config directory")?
            .config_dir()
            .to_path_buf();
        Ok(config_dir.join("favorites.json"))
    }

    /// Load favorites from disk.
    pub fn load_favorites(&mut self) {
        if let Ok(path) = Self::favorites_file_path()
            && path.exists()
            && let Ok(contents) = std::fs::read_to_string(&path)
            && let Ok(file) = serde_json::from_str::<FavoritesFile>(&contents)
        {
            self.favorite_queries = file.favorites;
        }
    }

    /// Save favorites to disk.
    pub fn save_favorites(&self) {
        if let Ok(path) = Self::favorites_file_path() {
            if let Some(parent) = path.parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            let file = FavoritesFile {
                version: 1,
                favorites: self.favorite_queries.clone(),
            };
            if let Ok(contents) = serde_json::to_string_pretty(&file) {
                let _ = std::fs::write(&path, contents);
            }
        }
    }

    /// Check if current query is a favorite.
    pub fn is_current_query_favorite(&self) -> bool {
        let trimmed = self.query_input.trim();
        !trimmed.is_empty() && self.favorite_queries.iter().any(|f| f.trim() == trimmed)
    }

    /// Toggle favorite status for current query.
    pub fn toggle_favorite(&mut self) {
        let trimmed = self.query_input.trim().to_string();
        if trimmed.is_empty() {
            return;
        }

        if let Some(idx) = self
            .favorite_queries
            .iter()
            .position(|f| f.trim() == trimmed)
        {
            self.favorite_queries.remove(idx);
        } else {
            self.favorite_queries.push(trimmed);
        }
    }

    /// Remove a favorite by index.
    pub fn remove_favorite(&mut self, idx: usize) {
        if idx < self.favorite_queries.len() {
            self.favorite_queries.remove(idx);
            // Adjust panel index if needed
            if self.favorites_panel_index >= self.favorite_queries.len()
                && !self.favorite_queries.is_empty()
            {
                self.favorites_panel_index = self.favorite_queries.len() - 1;
            }
        }
    }
}
