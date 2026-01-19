# Ampcc - Amp Command & Control TUI

## Overview

**Ampcc** (Amp Command & Control) is a terminal-based user interface (TUI) for managing and monitoring Amp datasets and jobs. It provides an interactive dashboard for exploring datasets, monitoring ETL jobs, and managing worker nodes across both local Amp instances and the public dataset registry.

## Quick Reference

| Need to... | Look at... |
|------------|-----------|
| Understand overall architecture | [Architecture](#architecture) section |
| Modify UI rendering | `ui.rs` (~1,285 lines) |
| Add new keyboard shortcuts | `main.rs` event handling (~722 lines) |
| Change application state | `app.rs` (~1,035 lines) |
| Add registry API features | `registry.rs` (~247 lines) |
| Modify configuration | `config.rs` (~50 lines) |

## Architecture

### Module Structure

```
ampcc/
├── main.rs (722 lines)      # Event loop & input handling
├── app.rs (1,035 lines)     # Application state & business logic
├── ui.rs (1,285 lines)      # Ratatui rendering
├── config.rs (50 lines)     # Configuration management
└── registry.rs (247 lines)  # Registry API client
```

### Data Flow

```
User Input (keyboard/mouse)
    ↓
main.rs (event loop)
    ↓
app.rs (state updates)
    ↓
Async Tasks (background HTTP requests)
    ↓
mpsc channel (results back to main loop)
    ↓
ui.rs (render updated state)
    ↓
Terminal (display)
```

### External Integration

```
ampcc (TUI)
  ├── admin-client → Local Amp Instance
  │   └── HTTP API: Jobs, Workers, Datasets, Manifests
  │       └── Endpoints: localhost:1610 (default)
  │
  └── registry.rs → Public Registry API
      └── HTTPS API: Datasets, Versions, Manifests
          └── Endpoint: api.registry.amp.staging.thegraph.com (default)
```

## Key Components

### 1. Application State (`app.rs`)

**Purpose**: Core business logic and state management

**Key Types**:
- `App` - Main application state struct
  - Holds UI state, data, and clients
  - Manages dataset lists, job lists, worker lists
  - Tracks scroll positions, selection indices
  - Contains search state and error messages

- `ActivePane` - Focus tracking enum
  - `Header`, `Datasets`, `Jobs`, `Workers`, `Manifest`, `Schema`, `Detail`
  - Determines which section responds to keyboard input
  - Used by UI to highlight active section

- `ContentView` - What's displayed in content pane
  - `DatasetManifest` - JSON manifest with syntax highlighting
  - `DatasetSchema` - Parsed Arrow type information
  - `JobDetail` - Selected job information
  - `WorkerDetail` - Selected worker node details

- `DataSource` - Operating mode
  - `Local` - Connected to local Amp admin API
  - `Registry` - Connected to public dataset registry

- `DatasetEntry` - Unified dataset representation
  - Works for both Local and Registry sources
  - Contains: namespace, name, description, URL
  - Tracks expansion state (collapsed/expanded with versions)
  - Version list populated on demand

- `VersionEntry` - Dataset version information
  - Version string, status, timestamp
  - Optional digest for validation

- `InspectResult` - Parsed schema from manifest
  - Extracts Arrow field definitions
  - Formats complex types for display

**Key Behaviors**:
- `fetch_datasets()` - Load from Local or Registry source
- `fetch_manifest()` - Retrieve dataset manifest JSON
- `inspect_manifest()` - Parse Arrow schema from manifest
- `toggle_dataset()` - Expand/collapse version list
- `get_selected_dataset()` - Map flat index to dataset
- `filter_datasets()` - Search by keyword
- `switch_source()` - Toggle between Local and Registry

### 2. Event Loop (`main.rs`)

**Purpose**: Terminal setup, input handling, async task coordination

**Key Features**:

**Terminal Management**:
- Raw mode enable/disable for direct keyboard control
- Alternate screen buffer for clean exit
- Mouse capture for click handling
- Panic handler ensures terminal restoration

**Event Handling**:
- Non-blocking event polling with `crossterm::event::poll()`
- Keyboard shortcut processing
- Mouse click detection for pane switching
- Async channel for background task results

**Async Task Spawning**:
- `spawn_fetch_manifest(dataset_ref)` - Get dataset manifest
- `spawn_fetch_jobs()` - List jobs from admin API
- `spawn_fetch_workers()` - List worker nodes
- `spawn_fetch_worker_detail(node_id)` - Get detailed worker info
- `spawn_stop_job(job_id)` - Stop a running job
- `spawn_delete_job(job_id)` - Delete a terminal job

**Auto-Refresh**:
- 10-second interval for jobs/workers in Local mode
- Controlled by `last_refresh` timestamp
- Only refreshes when in Local source

**Keyboard Shortcuts**:

| Key | Action | Notes |
|-----|--------|-------|
| `q` / `Ctrl+C` | Quit application | Restores terminal (cancels streaming if active) |
| `1` | Switch to Local source | Loads local datasets |
| `2` | Switch to Registry source | Loads registry datasets |
| `/` | Enter search mode | Search datasets by keyword |
| `Esc` | Exit search mode | Clear search filter |
| `r` | Refresh | Context-sensitive refresh |
| `Tab` | Next pane | Cycles through panes |
| `Shift+Tab` | Previous pane | Reverse cycle |
| `↑` / `k` | Move up | Navigate lists |
| `↓` / `j` | Move down | Navigate lists |
| `Ctrl+U` | Page up | Half-page scroll |
| `Ctrl+D` | Page down | Half-page scroll |
| `Enter` | Select/Expand | Expand dataset or show details |
| `s` | Stop job | Only in Jobs pane, for running jobs |
| `d` | Delete job | Only in Jobs pane, for terminal jobs |
| `Q` | Open SQL query panel | In Datasets pane (Local mode only) |
| `Ctrl+Enter` | Execute query | In Query input mode |
| `Ctrl+R` | History search | Reverse search through query history |
| `Ctrl+F` | Favorites panel | Open/close favorites panel |
| `*` / `F` | Toggle favorite | Add/remove current query from favorites |
| `T` | Template picker | Open query template selection |
| `E` | Export results | Export query results to CSV |
| `s` | Sort by column | In Query Results pane |
| `S` | Clear sort | In Query Results pane |

**Mouse Support**:
- Click to switch active pane
- Click to select items in lists
- Automatic focus management

### 3. UI Rendering (`ui.rs`)

**Purpose**: All terminal UI rendering using Ratatui framework

**Color Theme** (The Graph's official palette):
- Primary: Purple `#6F4CFF`, Dark `#0C0A1D`, Gray `#494755`
- Secondary: Blue `#4C66FF`, Aqua `#66D8FF`, Green `#4BCA81`, Pink `#FF79C6`, Yellow `#FFA801`
- Status: Green for success, red for errors, yellow for warnings

**Layout Structure**:
```
┌─────────────────────────────────────────────┐
│ Header (source info, status)                │  ~3 lines
├─────────────┬───────────────────────────────┤
│             │                               │
│  Sidebar    │      Content Pane            │  Main area
│  (35%)      │      (65%)                   │  (remaining height)
│             │                               │
│  - Datasets │  - Dataset Manifest          │
│  - Jobs     │  - Dataset Schema            │
│  - Workers  │  - Job Details               │
│             │  - Worker Details            │
├─────────────┴───────────────────────────────┤
│ Footer (help text, status messages)         │  1 line
└─────────────────────────────────────────────┘
```

**Key Rendering Functions**:

- `draw(frame, app)` - Main entry point
  - Lays out header/main/footer sections
  - Routes to splash or main view

- `draw_header(frame, area, app)` - Status bar
  - Shows current source (Local/Registry)
  - Displays error messages if present
  - Source indicators with color coding

- `draw_splash(frame, area)` - Welcome screen
  - The Graph ASCII logo
  - Instructions to switch sources
  - Displayed until data loads

- `draw_main(frame, area, app)` - Main layout manager
  - Splits into sidebar (35%) and content (65%)
  - Routes to Local or Registry sidebar

- `draw_sidebar_local(frame, area, app)` - Local mode sidebar
  - Three sections: Datasets, Jobs, Workers
  - Dynamic height allocation
  - Scrollable lists with highlighting

- `draw_sidebar_registry(frame, area, app)` - Registry mode sidebar
  - Single section: Datasets only
  - Full height for dataset list

- `draw_datasets_section(frame, area, app, is_active)` - Dataset list
  - Expandable tree view (▾ expanded, ▸ collapsed)
  - Shows versions when expanded
  - Indented version entries
  - Search filtering with match count

- `draw_jobs_section(frame, area, app, is_active)` - Job list
  - Status icons: ▶ Running, ◷ Pending, ✓ Success, ✗ Failed
  - Color-coded status (green/yellow/red)
  - Job ID and dataset reference

- `draw_workers_section(frame, area, app, is_active)` - Worker list
  - Node ID display
  - Status indicator
  - Selection highlighting

- `draw_content(frame, area, app)` - Content display
  - Routes to manifest, schema, job detail, or worker detail
  - Scrollable content with scrollbar
  - Syntax highlighting for JSON
  - Formatted table for schemas

- `draw_footer(frame, area, app)` - Status/help text
  - Search mode indicator
  - Context-sensitive help text
  - Status messages

**UI Patterns**:

- **Active Pane Highlighting**: Aqua border when focused, gray when not
- **Scrollbar Indicators**: Shows position in long content
- **Responsive Layout**: Adjusts to terminal size
- **Smart Selection**: Highlights selected items with different background
- **Status Icons**: Visual feedback for job states
- **Expandable Lists**: Toggle visibility of child items
- **Search Highlighting**: Shows match count in header

### 4. Configuration Management (`config.rs`)

**Purpose**: Load and manage application configuration

**Configuration Sources** (priority order):
1. Configuration file: `~/.config/ampcc/config.toml`
2. Environment variables: `AMP_CC_*` prefix

**Configuration Options**:

```rust
pub struct Config {
    /// gRPC endpoint for local query service
    /// Default: "grpc://localhost:1602"
    /// Env: AMP_CC_LOCAL_QUERY_URL
    pub local_query_url: String,

    /// HTTP endpoint for local admin API
    /// Default: "http://localhost:1610"
    /// Env: AMP_CC_LOCAL_ADMIN_URL
    pub local_admin_url: String,

    /// HTTPS endpoint for public registry API
    /// Default: "https://api.registry.amp.staging.thegraph.com"
    /// Env: AMP_CC_REGISTRY_URL
    pub registry_url: String,

    /// Which source to load on startup ("local" or "registry")
    /// Default: "registry"
    /// Env: AMP_CC_DEFAULT_SOURCE
    pub default_source: String,
}
```

**Example Configuration File**:

```toml
# ~/.config/ampcc/config.toml
local_query_url = "grpc://localhost:1602"
local_admin_url = "http://localhost:1610"
registry_url = "https://api.registry.amp.staging.thegraph.com"
default_source = "registry"
```

**Loading with Figment**:
- Merges configuration from multiple sources
- Environment variables override file values
- Provides sensible defaults for all options

### 5. Registry API Client (`registry.rs`)

**Purpose**: HTTP client for the public Amp dataset registry

**Key Types**:

- `RegistryClient` - HTTP client wrapper
  - Built on `reqwest::Client`
  - Optional auth token support
  - Base URL configuration

- `RegistryDataset` - Dataset from registry API
  - Namespace, name, description
  - URLs, metadata
  - Version count, latest version

- `RegistryVersion` - Version information
  - Version string, digest
  - Created timestamp
  - Size, file count

- `RegistryError` - Error handling
  - HTTP errors, JSON parsing errors
  - Connection failures

**API Methods**:

```rust
impl RegistryClient {
    /// Create new client with base URL
    pub fn new(base_url: impl Into<String>) -> Result<Self, RegistryError>

    /// Create client with auth token
    pub fn with_auth_token(base_url: impl Into<String>, token: String) -> Result<Self, RegistryError>

    /// List datasets (paginated)
    pub async fn list_datasets(&self, page: u32) -> Result<Vec<RegistryDataset>, RegistryError>

    /// Search datasets by keyword
    pub async fn search_datasets(&self, query: &str) -> Result<Vec<RegistryDataset>, RegistryError>

    /// Get versions for a dataset
    pub async fn get_versions(&self, namespace: &str, name: &str) -> Result<Vec<RegistryVersion>, RegistryError>

    /// Get manifest JSON for a dataset version
    pub async fn get_manifest(&self, namespace: &str, name: &str, version: &str) -> Result<String, RegistryError>

    /// Test connection to registry
    pub async fn test_connection(&self) -> Result<(), RegistryError>

    /// Auto-load auth token from file system
    pub fn load_auth_token() -> Option<String>
}
```

**Authentication**:
- Reads token from `AMP_AUTH_TOKEN` environment variable
- Falls back to `~/.amp/cache/amp_cli_auth` file
- Sends as Bearer token in Authorization header
- Optional - client works without auth for public datasets

**API Endpoints**:
- `GET /datasets?page={page}` - List datasets
- `GET /datasets/search?q={query}` - Search datasets
- `GET /datasets/{namespace}/{name}/versions` - List versions
- `GET /datasets/{namespace}/{name}/versions/{version}/manifest` - Get manifest

## Dependencies

### External Crates

| Crate | Purpose | Notes |
|-------|---------|-------|
| `ratatui` (0.30) | Terminal UI framework | Core rendering engine |
| `crossterm` (0.29.0) | Terminal control | Event handling, raw mode |
| `tokio` | Async runtime | Background tasks |
| `reqwest` | HTTP client | Registry API calls |
| `serde` / `serde_json` | Serialization | JSON parsing |
| `figment` | Configuration | Multi-source config loading |
| `thiserror` | Error handling | Error type derivation |
| `anyhow` | Error handling | Error context |
| `directories` (6.0) | Path resolution | Config file location |
| `urlencoding` (2.1) | URL encoding | Search query encoding |

### Internal Amp Crates

| Crate | Purpose | Types/Functions Used |
|-------|---------|---------------------|
| `admin-client` | Admin API client | `Client`, `JobInfo`, `WorkerInfo`, `WorkerDetailResponse` |
| `worker` | Worker types | `JobId`, `NodeId` |
| `datasets-common` | Dataset types | `Reference`, `Revision` |

## Development Guidelines

### Adding New Features

**1. Adding a Keyboard Shortcut**:
- Edit `main.rs` in the event handling section
- Add case to `match event` block
- Update `draw_footer()` in `ui.rs` with help text
- Consider context sensitivity (which pane is active)

**2. Adding a New Pane**:
- Add variant to `ActivePane` enum in `app.rs`
- Add tab navigation logic in `main.rs`
- Create `draw_xxx_section()` function in `ui.rs`
- Update layout in `draw_sidebar_local()` or `draw_main()`

**3. Adding Registry API Feature**:
- Add method to `RegistryClient` in `registry.rs`
- Define response types with `#[derive(Deserialize)]`
- Handle errors with `RegistryError`
- Add async spawn function in `main.rs`
- Update `App` state in `app.rs` to store results

**4. Modifying UI Layout**:
- Edit `ui.rs` rendering functions
- Use Ratatui's `Layout::default()` for splitting
- Maintain color theme constants at top of file
- Test with different terminal sizes

### Testing Locally

**Prerequisites**:
- Local Amp instance running (ampd, controller, workers)
- Admin API accessible at `http://localhost:1610` (or configured URL)
- Optional: Auth token for registry access

**Running**:
```bash
# Run with default config
cargo run -p ampcc

# Run with custom config
AMP_CC_LOCAL_ADMIN_URL=http://localhost:8080 cargo run -p ampcc

# Run with auth token
AMP_AUTH_TOKEN=your_token_here cargo run -p ampcc
```

**Testing Checklist**:
- [ ] Both sources (Local and Registry) load data
- [ ] Dataset expansion shows versions
- [ ] Manifest and schema display correctly
- [ ] Jobs list updates (if Local mode)
- [ ] Workers list updates (if Local mode)
- [ ] Search filtering works
- [ ] Keyboard navigation responsive
- [ ] Mouse clicks work
- [ ] Auto-refresh works (10-second interval)
- [ ] Job stop/delete operations work
- [ ] Terminal restores properly on exit

### Code Style

**Follow Amp project conventions**:
- Use `/code-format` skill after editing
- Use `/code-check` skill for compilation and clippy
- Fix all clippy warnings before committing
- Use `thiserror` for error types
- Use `anyhow` for error context in functions
- Prefer `async/await` over callbacks

**UI-Specific Conventions**:
- Keep color constants at top of `ui.rs`
- Use descriptive function names: `draw_xxx_section()`
- Separate layout logic from rendering logic
- Document complex UI calculations

### Performance Considerations

**Efficient Rendering**:
- Only redraw when `needs_redraw` flag is true
- Reset flag after successful render
- Avoid unnecessary allocations in render functions

**Async Task Management**:
- Spawn tasks for HTTP requests (don't block UI)
- Use channels for result communication
- Handle task failures gracefully

**State Management**:
- Lazy load version lists (only when dataset expanded)
- Cache manifest and schema results
- Clear stale data when switching sources

## Common Tasks

### Debugging Connection Issues

**Local Admin API not responding**:
1. Check if ampd is running: `ps aux | grep ampd`
2. Verify admin API port: `netstat -an | grep 1610`
3. Test endpoint: `curl http://localhost:1610/api/datasets`
4. Check config: `cat ~/.config/ampcc/config.toml`
5. Override with env var: `AMP_CC_LOCAL_ADMIN_URL=http://localhost:8080`

**Registry API failing**:
1. Test connection: `curl https://api.registry.amp.staging.thegraph.com/datasets`
2. Check auth token: `cat ~/.amp/cache/amp_cli_auth`
3. Set token manually: `AMP_AUTH_TOKEN=your_token`
4. Review error message in ampcc header

### Adding Custom Color Theme

Edit color constants in `ui.rs`:

```rust
const COLOR_PRIMARY: Color = Color::Rgb(111, 76, 255);  // #6F4CFF
const COLOR_SECONDARY: Color = Color::Rgb(76, 102, 255); // #4C66FF
// ... modify as needed
```

### Extending Dataset Display

To show additional dataset metadata:

1. Update `DatasetEntry` struct in `app.rs` with new fields
2. Modify `fetch_datasets()` to populate new fields from API
3. Update `draw_datasets_section()` in `ui.rs` to render new fields
4. Adjust layout to accommodate additional text

## Architecture Patterns

### Async Task Pattern

All HTTP requests follow this pattern:

```rust
// 1. Spawn async task
let tx = tx.clone();
let client = client.clone();
tokio::spawn(async move {
    // 2. Make HTTP request
    let result = client.some_request().await;

    // 3. Send result back via channel
    let _ = tx.send(AppEvent::SomeResult(result)).await;
});

// 4. In main loop, receive result
match rx.recv().await {
    Some(AppEvent::SomeResult(result)) => {
        // 5. Update app state
        app.update_from_result(result);
        needs_redraw = true;
    }
    // ...
}
```

**Benefits**:
- UI never blocks
- Multiple requests can run concurrently
- Clean separation of async and UI code

### State Update Pattern

All state modifications go through `App` methods:

```rust
impl App {
    // Encapsulate state changes
    pub fn set_datasets(&mut self, datasets: Vec<DatasetEntry>) {
        self.datasets = datasets;
        self.datasets_selected = 0;
        self.datasets_scroll_state = ScrollState::default();
    }

    // Validate state consistency
    pub fn select_dataset(&mut self, index: usize) {
        if index < self.datasets.len() {
            self.datasets_selected = index;
        }
    }
}
```

**Benefits**:
- Single source of truth
- State consistency guaranteed
- Easier to debug and test

### Error Handling Pattern

Use `Result` types throughout:

```rust
// Define error types with thiserror
#[derive(Debug, thiserror::Error)]
pub enum RegistryError {
    #[error("HTTP request failed: {0}")]
    RequestFailed(#[from] reqwest::Error),

    #[error("JSON parse error: {0}")]
    JsonError(#[from] serde_json::Error),
}

// Return errors, don't panic
pub async fn fetch_data(&self) -> Result<Data, RegistryError> {
    let response = self.client.get(url).send().await?;
    let data = response.json().await?;
    Ok(data)
}

// Handle errors in main loop
match fetch_data().await {
    Ok(data) => app.set_data(data),
    Err(e) => app.set_error(format!("Failed: {}", e)),
}
```

**Benefits**:
- Explicit error handling
- Clean error propagation with `?`
- User-friendly error messages

## SQL Query Panel

The SQL Query Panel allows users to execute SQL queries against datasets in Local mode.

### Features

**Query Input**:
- Multi-line SQL input with syntax highlighting
- Enter for newlines, Ctrl+Enter to execute
- Query history navigation with Up/Down arrows
- Reverse history search with Ctrl+R
- Query templates with `T` key

**Query Results**:
- Streaming results display with progressive loading
- Results appear as batches arrive from Flight service
- Real-time row count updates during streaming
- Ctrl+C cancels streaming query mid-execution
- Sortable columns with `s` key + column number
- Export to CSV with `E` key
- Auto-sized column widths based on content

**Favorites**:
- Toggle favorite with `*` or `F` key
- View favorites panel with Ctrl+F
- Persistent storage to `~/.config/ampcc/favorites.json`

### Streaming Query Execution

Queries execute via Flight protocol with streaming results:

```
┌─────────────────────────────────────────────────────────────┐
│ Query Results (streaming... 1,234 rows) [Ctrl+C to cancel]  │
├─────────────────────────────────────────────────────────────┤
│ block_number  │ hash                                        │
│ 1000001       │ 0xabc123...                                 │
│ 1000002       │ 0xdef456...                                 │
│ ... (streaming)                                             │
└─────────────────────────────────────────────────────────────┘
```

**State during streaming**:
- `app.query_streaming`: true while results are streaming
- `app.query_cancel_token`: CancellationToken for Ctrl+C cancellation
- `results.streaming`: true while batches are arriving
- `results.stream_complete`: true when all batches received

**Events**:
- `QueryColumnsReceived`: First batch, initializes columns
- `QueryBatchReceived`: Subsequent batches, appends rows
- `QueryStreamComplete`: All batches received
- `QueryCancelled`: User cancelled with Ctrl+C

### History Storage

Query history persisted to `~/.config/ampcc/history.json`:
- Per-dataset history with global fallback
- Automatic save on exit, load on startup
- Search with Ctrl+R

## Known Limitations

1. **No pagination UI** - Registry datasets fetched one page at a time, no UI for next/previous page
2. **Search is client-side** - Filters loaded datasets, doesn't use registry search API
3. **No worker detail in Registry mode** - Worker management only works with Local source
4. **No job creation UI** - Can only monitor/stop/delete existing jobs
5. **No configuration UI** - Must edit config file or set env vars manually
6. **Limited schema parsing** - Only handles Arrow types, doesn't show full dataset metadata

## Future Enhancement Ideas

- [ ] Add pagination controls for registry datasets
- [ ] Server-side search using registry API
- [ ] Job creation wizard (select dataset, configure params)
- [ ] Configuration editor (interactive TUI for settings)
- [ ] Enhanced schema viewer (show constraints, metadata)
- [ ] Job logs viewer (tail job output in real-time)
- [ ] Worker metrics dashboard (CPU, memory, job count)
- [ ] Export dataset list to CSV/JSON
- [ ] Bookmark favorite datasets
- [ ] Multi-source comparison view

## Troubleshooting

### Terminal Display Issues

**Symptoms**: Garbled text, incorrect colors, layout broken
**Solutions**:
- Ensure terminal supports 256 colors: `echo $TERM` should show `xterm-256color` or similar
- Resize terminal and press `r` to refresh
- Try different terminal emulator (iTerm2, Alacritty, etc.)

### Compilation Errors

**Missing dependencies**:
```bash
# Update dependencies
cargo update -p ampcc

# Rebuild from scratch
cargo clean && cargo build -p ampcc
```

**Clippy warnings**:
```bash
# Run clippy
just check

# Auto-fix some warnings
cargo clippy --fix -p ampcc
```

### Runtime Panics

**Terminal not restored**:
- Force restore: `reset` command in shell
- Or: Close terminal and open new one

**Panic on startup**:
- Check config file syntax: `cat ~/.config/ampcc/config.toml`
- Verify URLs are well-formed
- Check auth token file exists: `ls ~/.amp/cache/amp_cli_auth`

## Related Documentation

- **Project Architecture**: `/docs/architecture.md`
- **Admin API Spec**: `/docs/openapi-specs/admin-api.yaml`
- **Dataset Patterns**: `/.patterns/datasets/`
- **Service Patterns**: `/.patterns/services-pattern.md`
