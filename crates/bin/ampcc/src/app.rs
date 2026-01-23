//! Application state and business logic.

use std::{collections::HashMap, sync::Arc, time::Instant};

use admin_client::{
    Client,
    jobs::JobInfo,
    workers::{WorkerDetailResponse, WorkerInfo},
};
use anyhow::{Context, Result};
use ratatui::widgets::ScrollbarState;
use reqwest::Client as HttpClient;
use tokio::sync::mpsc::UnboundedSender;
use url::Url;

use crate::{action::Action, auth::AuthStorage, config::Config, registry::RegistryClient};

/// Input mode for the application.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputMode {
    Normal,
    Search,
}

/// Active pane for focus tracking.
/// In Local mode: Header -> Datasets -> Jobs -> Workers -> Detail -> Header
/// In Registry mode: Header -> Datasets -> Manifest -> Schema -> Header
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActivePane {
    Header,
    Datasets,
    Jobs,     // Local only
    Workers,  // Local only
    Manifest, // Dataset manifest pane (content area)
    Schema,   // Dataset schema pane (content area)
    Detail,   // Job/Worker detail view (Local only)
}

impl ActivePane {
    /// Cycle to the next pane.
    /// Local mode: Header -> Datasets -> Jobs -> Workers -> Detail -> Header
    /// Registry mode: Header -> Datasets -> Manifest -> Schema -> Header
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
        }
    }

    /// Cycle to the previous pane.
    /// Local mode: Header -> Detail -> Workers -> Jobs -> Datasets -> Header
    /// Registry mode: Header -> Schema -> Manifest -> Datasets -> Header
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
    /// Nothing selected
    None,
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

/// Status of the device flow authentication.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeviceFlowStatus {
    /// Waiting for user to confirm (press Enter to open browser).
    AwaitingConfirmation,
    /// Browser opened, waiting for user to complete auth.
    WaitingForBrowser,
    /// Actively polling for token.
    Polling,
    /// Error occurred while opening the user's browser.
    ///
    /// Print the Auth URL so they can open it manually.
    OpenBrowserFailure(String),
    /// Error occurred during device flow.
    Error(String),
}

/// State for the device flow authentication process.
#[derive(Debug, Clone)]
pub struct DeviceFlowState {
    /// User code to display/copy.
    pub user_code: String,
    /// URL where user authenticates.
    pub verification_uri: String,
    /// Device code for polling.
    pub device_code: String,
    /// PKCE code verifier.
    pub code_verifier: String,
    /// Polling interval in seconds.
    pub interval: i64,
    /// Current status.
    pub status: DeviceFlowStatus,
    /// If copy-to-clipboard threw an error, display in auth_screen
    pub copy_to_clipboard_failed: bool,
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

    // Action channel for async task communication
    pub action_tx: UnboundedSender<Action>,

    // HTTP client (shared with auth clients)
    pub http_client: HttpClient,

    // Clients
    pub local_client: Arc<Client>,
    pub registry_client: RegistryClient,

    // Auth state
    pub auth_state: Option<AuthStorage>,
    pub auth_device_flow: Option<DeviceFlowState>,

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
}

impl App {
    /// Create a new application instance.
    pub fn new(
        config: Config,
        action_tx: UnboundedSender<Action>,
        http_client: HttpClient,
    ) -> Result<Self> {
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
            action_tx,
            http_client,
            local_client,
            registry_client,
            auth_state: None,
            auth_device_flow: None,
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
        })
    }

    /// Spinner frames for loading animation (braille pattern).
    pub const SPINNER_FRAMES: &'static [char] = &['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'];

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
            ActivePane::Header | ActivePane::Datasets | ActivePane::Jobs | ActivePane::Workers => {}
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
            ActivePane::Header | ActivePane::Datasets | ActivePane::Jobs | ActivePane::Workers => {}
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
        self.manifest_content_length = 0;
        self.schema_content_length = 0;
        self.detail_content_length = 0;
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
            ActivePane::Header | ActivePane::Datasets | ActivePane::Jobs | ActivePane::Workers => {}
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
            ActivePane::Header | ActivePane::Datasets | ActivePane::Jobs | ActivePane::Workers => {}
        }
    }

    /// Get the current source URL for display.
    pub fn current_source_url(&self) -> &str {
        match self.current_source {
            DataSource::Local => &self.config.local_admin_url,
            DataSource::Registry => &self.config.registry_url,
        }
    }

    /// Fetch datasets from local admin API.
    ///
    /// This is an associated function that can be called from spawn tasks.
    pub async fn fetch_local_datasets(client: &admin_client::Client) -> Result<Vec<DatasetEntry>> {
        let response = client.datasets().list_all().await?;
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
    ///
    /// This is an associated function that can be called from spawn tasks.
    pub async fn fetch_registry_datasets(
        client: &crate::registry::RegistryClient,
    ) -> Result<Vec<DatasetEntry>> {
        let mut all_datasets = Vec::new();
        let mut page = 1;

        loop {
            let response = client.list_datasets(page).await?;
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

    /// Fetch versions for a dataset from the registry.
    ///
    /// This is an associated function that can be called from spawn tasks.
    pub async fn fetch_registry_versions(
        client: &crate::registry::RegistryClient,
        namespace: &str,
        name: &str,
    ) -> Result<Vec<VersionEntry>> {
        let versions = client.get_versions(namespace, name).await?;
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

    /// Check if the application is still running.
    pub fn is_running(&self) -> bool {
        !self.should_quit
    }

    /// Send an action to the action channel.
    pub fn send_action(&self, action: Action) {
        let _ = self.action_tx.send(action);
    }

    /// Handle an action and mutate state accordingly.
    pub fn handle_action(&mut self, action: Action) {
        match action {
            Action::None => {}

            Action::Quit => self.should_quit = true,

            // Navigation
            Action::NavigateDown => match self.active_pane {
                ActivePane::Datasets => {
                    self.select_next();
                    self.send_action(Action::LoadManifest);
                }
                ActivePane::Jobs => {
                    self.select_next_job();
                    if let Some(job) = self.get_selected_job().cloned() {
                        self.content_view = ContentView::Job(job);
                        self.reset_scroll();
                    }
                }
                ActivePane::Workers => {
                    self.select_next_worker();
                    if let Some(node_id) = self.get_selected_worker().map(|w| w.node_id.clone()) {
                        self.send_action(Action::LoadWorkerDetail(node_id));
                    }
                }
                _ => self.scroll_down(),
            },

            Action::NavigateUp => match self.active_pane {
                ActivePane::Datasets => {
                    self.select_previous();
                    self.send_action(Action::LoadManifest);
                }
                ActivePane::Jobs => {
                    self.select_previous_job();
                    if let Some(job) = self.get_selected_job().cloned() {
                        self.content_view = ContentView::Job(job);
                        self.reset_scroll();
                    }
                }
                ActivePane::Workers => {
                    self.select_previous_worker();
                    if let Some(node_id) = self.get_selected_worker().map(|w| w.node_id.clone()) {
                        self.send_action(Action::LoadWorkerDetail(node_id));
                    }
                }
                _ => self.scroll_up(),
            },

            Action::PageDown(size) => self.page_down(size),
            Action::PageUp(size) => self.page_up(size),

            Action::NextPane => {
                let is_local = self.is_local();
                self.active_pane = self.active_pane.next(is_local);
            }

            Action::PrevPane => {
                let is_local = self.is_local();
                self.active_pane = self.active_pane.prev(is_local);
            }

            Action::ToggleExpand => {
                // This needs async - will be handled by spawning a task
                // For now, mark as needing implementation
            }

            Action::EnterDetail => match self.active_pane {
                ActivePane::Jobs => {
                    if let Some(job) = self.get_selected_job().cloned() {
                        self.content_view = ContentView::Job(job);
                        self.reset_scroll();
                        self.active_pane = ActivePane::Detail;
                    }
                }
                ActivePane::Workers => {
                    if let Some(node_id) = self.get_selected_worker().map(|w| w.node_id.clone()) {
                        self.send_action(Action::LoadWorkerDetail(node_id));
                        self.active_pane = ActivePane::Detail;
                    }
                }
                _ => {}
            },

            // Source switching
            Action::SwitchToLocal => {
                if self.current_source != DataSource::Local {
                    self.start_loading("Switching source...");
                    // Spawns async task handled by handle_async_action
                }
            }

            Action::SwitchToRegistry => {
                if self.current_source != DataSource::Registry {
                    self.start_loading("Switching source...");
                    // Spawns async task handled by handle_async_action
                }
            }

            Action::SourceSwitched(result) => match result {
                Ok(source) => {
                    self.current_source = source;
                    self.search_query.clear();
                    self.selected_index = 0;
                    self.selected_version_indices.clear();
                    self.current_manifest = None;

                    if source == DataSource::Registry {
                        self.jobs.clear();
                        self.workers.clear();
                        self.selected_job_index = 0;
                        self.selected_worker_index = 0;
                        self.content_view = ContentView::None;
                        if matches!(self.active_pane, ActivePane::Jobs | ActivePane::Workers) {
                            self.active_pane = ActivePane::Datasets;
                        }
                    }

                    self.send_action(Action::RefreshDatasets);
                }
                Err(e) => {
                    self.error_message = Some(e);
                    self.stop_loading();
                }
            },

            // Search
            Action::EnterSearchMode => {
                self.input_mode = InputMode::Search;
            }

            Action::ExitSearchMode => {
                self.input_mode = InputMode::Normal;
            }

            Action::SearchInput(c) => {
                self.search_query.push(c);
                self.update_search();
            }

            Action::SearchBackspace => {
                self.search_query.pop();
                self.update_search();
            }

            Action::SearchSubmit => {
                self.input_mode = InputMode::Normal;
                self.send_action(Action::LoadManifest);
            }

            // Datasets
            Action::RefreshDatasets => {
                self.start_loading("Refreshing datasets...");
                // Spawns async task to refresh datasets handled by handle_async_action
            }

            Action::DatasetsLoaded(result) => {
                match result {
                    Ok(datasets) => {
                        self.datasets = datasets;
                        self.update_search();
                        self.send_action(Action::LoadManifest);
                    }
                    Err(e) => {
                        self.error_message = Some(e);
                    }
                }
                self.stop_loading();
            }

            Action::VersionsLoaded {
                dataset_index,
                versions,
            } => {
                match versions {
                    Ok(v) => {
                        if let Some(dataset) = self.filtered_datasets.get_mut(dataset_index) {
                            dataset.versions = Some(v);
                            dataset.expanded = true;
                        }
                    }
                    Err(e) => {
                        self.error_message = Some(e);
                    }
                }
                self.stop_loading();
            }

            // Manifest
            Action::LoadManifest => {
                self.start_loading("Loading manifest...");
                // Spawns async task to load manifest handled by handle_async_action
            }

            Action::ManifestLoaded(manifest) => {
                self.reset_scroll();
                self.current_inspect = manifest.as_ref().and_then(InspectResult::from_manifest);
                self.current_manifest = manifest;
                self.content_view = ContentView::Dataset;
                self.stop_loading();
            }

            // Jobs
            Action::RefreshJobs => {}

            Action::JobsLoaded(jobs) => {
                self.jobs = jobs;
                if self.selected_job_index >= self.jobs.len() && !self.jobs.is_empty() {
                    self.selected_job_index = self.jobs.len() - 1;
                }
            }

            Action::StopJob(_job_id) => {
                self.start_loading("Stopping job...");
                // Spawns async task to stop job handled by handle_async_action
            }

            Action::JobStopped(result) => {
                match result {
                    Ok(()) => self.send_action(Action::RefreshJobs),
                    Err(e) => self.error_message = Some(e),
                }
                self.stop_loading();
            }

            Action::DeleteJob(_job_id) => {
                self.start_loading("Deleting job...");
                // Spawns async task to delete job handled by handle_async_action
            }

            Action::JobDeleted(result) => {
                match result {
                    Ok(()) => self.send_action(Action::RefreshJobs),
                    Err(e) => self.error_message = Some(e),
                }
                self.stop_loading();
            }

            // Workers
            Action::RefreshWorkers => {}

            Action::WorkersLoaded(workers) => {
                self.workers = workers;
                if self.selected_worker_index >= self.workers.len() && !self.workers.is_empty() {
                    self.selected_worker_index = self.workers.len() - 1;
                }
            }

            Action::LoadWorkerDetail(_node_id) => {
                self.start_loading("Loading worker details...");
                // Spawns async task to load worker details handled by handle_async_action
            }

            Action::WorkerDetailLoaded(detail) => {
                if let Some(worker_detail) = detail {
                    self.content_view = ContentView::Worker(worker_detail);
                    self.reset_scroll();
                }
                self.stop_loading();
            }

            // Auth
            Action::AuthCheckOnStartup => {}

            Action::AuthStateLoaded(auth) => {
                // Update registry client with the loaded auth token
                if let Some(ref a) = auth {
                    self.registry_client = RegistryClient::with_token(
                        self.config.registry_url.clone(),
                        Some(a.access_token.clone()),
                    );
                }
                self.auth_state = auth;
            }

            Action::AuthLogin => {
                if self.auth_state.is_none() && self.auth_device_flow.is_none() {
                    self.start_loading("Logging in...");
                    // Spawns async task to log user in with auth flow handled by handle_async_action
                }
            }

            Action::AuthLogout => {
                let _ = AuthStorage::clear();
                self.auth_state = None;
                self.auth_device_flow = None;
                // Rebuild registry client without explicit token (falls back to env var)
                self.registry_client =
                    RegistryClient::with_token(self.config.registry_url.clone(), None);
            }

            Action::AuthDeviceFlowPending {
                user_code,
                verification_uri,
                device_code,
                code_verifier,
                interval,
            } => {
                self.stop_loading();

                let mut copy_to_clipboard_failed = false;
                // Copy user code to clipboard
                if let Ok(mut clipboard) = arboard::Clipboard::new() {
                    if clipboard.set_text(&user_code).is_err() {
                        copy_to_clipboard_failed = true;
                    }
                } else {
                    copy_to_clipboard_failed = true;
                }

                self.auth_device_flow = Some(DeviceFlowState {
                    user_code,
                    verification_uri,
                    device_code,
                    code_verifier,
                    interval,
                    status: DeviceFlowStatus::AwaitingConfirmation,
                    copy_to_clipboard_failed,
                });
            }

            Action::AuthDeviceFlowConfirm => {
                if let Some(ref mut flow) = self.auth_device_flow {
                    if crate::auth::PkceDeviceFlowClient::open_browser(&flow.verification_uri)
                        .is_err()
                    {
                        // pass the auth URL to the error to print in the auth screen
                        flow.status =
                            DeviceFlowStatus::OpenBrowserFailure(self.config.auth_url.clone());
                    } else {
                        flow.status = DeviceFlowStatus::WaitingForBrowser;
                    }
                    // Clone values before sending to avoid borrow conflict
                    let device_code = flow.device_code.clone();
                    let code_verifier = flow.code_verifier.clone();
                    let interval = flow.interval;
                    self.send_action(Action::AuthDeviceFlowPoll {
                        device_code,
                        code_verifier,
                        interval,
                        is_first_poll: true,
                    });
                }
            }

            Action::AuthDeviceFlowPoll {
                device_code,
                code_verifier,
                interval,
                is_first_poll,
            } => {
                if let Some(ref mut flow) = self.auth_device_flow {
                    flow.status = DeviceFlowStatus::Polling;
                }
                let _ = (device_code, code_verifier, interval, is_first_poll);
            }

            Action::AuthDeviceFlowComplete(auth) => {
                let _ = auth.save();
                // Update registry client with new auth token
                self.registry_client = RegistryClient::with_token(
                    self.config.registry_url.clone(),
                    Some(auth.access_token.clone()),
                );
                self.auth_state = Some(auth);
                self.auth_device_flow = None;
            }

            Action::AuthDeviceFlowCancel => {
                self.auth_device_flow = None;
            }

            Action::AuthError(error) => {
                if let Some(ref mut flow) = self.auth_device_flow {
                    flow.status = DeviceFlowStatus::Error(error);
                } else {
                    // Error during initial login request (before device flow started)
                    self.stop_loading();
                    self.error_message = Some(error);
                }
            }

            Action::AuthRefreshComplete(result) => match result {
                Ok(auth) => {
                    let _ = auth.save();
                    // Update registry client with refreshed token
                    self.registry_client = RegistryClient::with_token(
                        self.config.registry_url.clone(),
                        Some(auth.access_token.clone()),
                    );
                    self.auth_state = Some(auth);
                }
                Err(_) => {
                    let _ = AuthStorage::clear();
                    self.auth_state = None;
                    // Rebuild registry client without explicit token (falls back to env var)
                    self.registry_client =
                        RegistryClient::with_token(self.config.registry_url.clone(), None);
                }
            },

            // Errors
            Action::Error(msg) => {
                self.error_message = Some(msg);
                self.stop_loading();
            }
        }

        self.needs_redraw = true;
    }
}
