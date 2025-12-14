//! Application state and business logic.

use std::{collections::HashMap, sync::Arc};

use admin_client::Client;
use anyhow::{Context, Result};
use ratatui::widgets::ScrollbarState;
use url::Url;

use crate::{config::Config, registry::RegistryClient};

/// Input mode for the application.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputMode {
    Normal,
    Search,
}

/// Active pane for focus tracking.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActivePane {
    Sidebar,
    Manifest,
    Schema,
}

impl ActivePane {
    /// Cycle to the next pane.
    pub fn next(self) -> Self {
        match self {
            ActivePane::Sidebar => ActivePane::Manifest,
            ActivePane::Manifest => ActivePane::Schema,
            ActivePane::Schema => ActivePane::Sidebar,
        }
    }

    /// Cycle to the previous pane.
    pub fn prev(self) -> Self {
        match self {
            ActivePane::Sidebar => ActivePane::Schema,
            ActivePane::Manifest => ActivePane::Sidebar,
            ActivePane::Schema => ActivePane::Manifest,
        }
    }
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
        if let Some(name) = obj.get("name").and_then(|v| v.as_str()) {
            match name {
                "timestamp" => {
                    let unit = obj
                        .get("unit")
                        .and_then(|v| v.as_str())
                        .unwrap_or("unknown");
                    let tz = obj
                        .get("timezone")
                        .and_then(|v| v.as_str())
                        .map(|s| format!(", {}", s))
                        .unwrap_or_default();
                    return format!("Timestamp({}{})", unit, tz);
                }
                "fixedsizebinary" => {
                    let size = obj.get("byteWidth").and_then(|v| v.as_u64()).unwrap_or(0);
                    return format!("FixedSizeBinary({})", size);
                }
                "fixedsizelist" => {
                    let size = obj.get("listSize").and_then(|v| v.as_u64()).unwrap_or(0);
                    let child = obj
                        .get("children")
                        .and_then(|c| c.as_array())
                        .and_then(|arr| arr.first())
                        .and_then(|c| c.get("type"))
                        .map(format_arrow_type)
                        .unwrap_or_else(|| "?".to_string());
                    return format!("FixedSizeList({}, {})", child, size);
                }
                "list" => {
                    let child = obj
                        .get("children")
                        .and_then(|c| c.as_array())
                        .and_then(|arr| arr.first())
                        .and_then(|c| c.get("type"))
                        .map(format_arrow_type)
                        .unwrap_or_else(|| "?".to_string());
                    return format!("List({})", child);
                }
                "struct" => {
                    return "Struct(...)".to_string();
                }
                _ => {
                    return name.to_string();
                }
            }
        }
    }

    "Unknown".to_string()
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

    // Manifest state
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

    // Content length for scroll bounds
    pub manifest_content_length: usize,
    pub schema_content_length: usize,
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
            active_pane: ActivePane::Sidebar,
            search_query: String::new(),
            datasets: Vec::new(),
            filtered_datasets: Vec::new(),
            selected_index: 0,
            selected_version_indices: HashMap::new(),
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
            manifest_content_length: 0,
            schema_content_length: 0,
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
            ActivePane::Sidebar => {}
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
            ActivePane::Sidebar => {}
        }
    }

    /// Reset scroll positions when content changes.
    pub fn reset_scroll(&mut self) {
        self.manifest_scroll = 0;
        self.manifest_scroll_state = ScrollbarState::default();
        self.schema_scroll = 0;
        self.schema_scroll_state = ScrollbarState::default();
        self.manifest_content_length = 0;
        self.schema_content_length = 0;
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
            ActivePane::Sidebar => {}
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
            ActivePane::Sidebar => {}
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

        self.fetch_datasets().await
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
            if dataset.expanded {
                if let Some(versions) = &dataset.versions {
                    count += versions.len();
                }
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

            if dataset.expanded {
                if let Some(versions) = &dataset.versions {
                    for version_idx in 0..versions.len() {
                        if current == flat_index {
                            return Some((dataset_idx, Some(version_idx)));
                        }
                        current += 1;
                    }
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
            if dataset.expanded {
                if let Some(versions) = &dataset.versions {
                    index += versions.len();
                }
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
                // Local doesn't have version listing in admin-client currently
                // Return just the latest version if available
                Ok(Vec::new())
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
}
