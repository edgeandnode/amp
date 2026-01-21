# SQL Query Panel

The SQL Query Panel in ampcc allows you to run SQL queries against local Amp datasets directly from the TUI.

> **Note**: This feature is only available in **Local mode** (press `1` to switch to Local mode).

## Getting Started

1. Switch to Local mode by pressing `1`
2. Select a dataset from the sidebar
3. Press `Q` to enter query mode

A query input panel will appear with a template query like:
```sql
SELECT * FROM namespace.dataset LIMIT 10
```

## Multi-line Query Input

The query panel supports multi-line SQL queries, making it easy to write formatted, readable queries.

### Keyboard Shortcuts

| Key | Action |
|-----|--------|
| `Enter` | Insert a new line |
| `Ctrl+Enter` | Execute the query |
| `Esc` | Cancel and return to normal mode |
| `↑` / `↓` | Navigate between lines (or browse history at first/last line) |
| `←` / `→` | Move cursor (wraps across line boundaries) |
| `Home` | Jump to start of current line |
| `End` | Jump to end of current line |
| `Backspace` | Delete before cursor (joins lines at line start) |
| `Delete` | Delete at cursor (joins lines at line end) |

### Example

```sql
SELECT
    block_number,
    transaction_hash,
    gas_used
FROM ethereum.transactions
WHERE block_number > 1000000
ORDER BY gas_used DESC
LIMIT 100
```

The input area automatically expands (up to 10 lines) to accommodate your query.

## Syntax Highlighting

SQL queries are automatically highlighted with colors to improve readability:

| Element | Color | Examples |
|---------|-------|----------|
| Keywords | Purple | `SELECT`, `FROM`, `WHERE`, `JOIN`, `ORDER BY` |
| Strings | Green | `'hello'`, `"world"` |
| Numbers | Yellow | `100`, `3.14` |
| Operators | Gray | `=`, `<`, `>`, `+`, `-` |
| Identifiers | White | table names, column names |

Highlighting updates as you type and works with both single and multi-line queries.

## Query History

Your queries are saved automatically and can be recalled:

- Press `↑` at the first line to browse older queries
- Press `↓` at the last line to browse newer queries
- Press `Ctrl+R` to search history by keyword
  - Type to filter matches
  - Press `Ctrl+R` again to cycle through matches
  - Press `Enter` to accept, `Esc` to cancel

### Per-Dataset History

History is organized **per-dataset** to keep your queries contextually relevant:

| Scenario | History Shown |
|----------|---------------|
| Dataset has queries | Dataset-specific history |
| New dataset (no history) | Empty (start fresh) |
| No dataset selected | Global history |

**How it works:**
- Each dataset (`namespace.name`) maintains its own isolated history
- When you execute a query, it's saved to the current dataset's history
- Switching datasets shows that dataset's history (or empty if new)
- Each dataset builds up its own relevant query history over time

**Example workflow:**
1. Select `eth.blocks`, press `Q` to enter query mode
2. Run `SELECT * FROM eth.blocks LIMIT 10`
3. Press `↑` → See the query you just ran
4. Switch to `eth.logs` (new dataset)
5. Press `↑` → Empty history (fresh start for this dataset)
6. Run `SELECT * FROM eth.logs WHERE topic = '...'`
7. Press `↑` → See only `eth.logs` queries
8. Switch back to `eth.blocks`, press `↑` → See only `eth.blocks` queries

### Persistent History

History is saved to `~/.config/ampcc/history.json` and persists across sessions:

```json
{
  "version": 2,
  "history": [...],           // Global history
  "dataset_history": {        // Per-dataset history
    "eth.blocks": [...],
    "eth.logs": [...]
  }
}
```

- Maximum 100 entries per dataset and 100 in global history
- Consecutive duplicate queries are not saved
- History loads on startup, saves on quit

## Query Templates

Press `T` while in query mode to open the template picker - a popup with common SQL patterns that auto-fill based on your selected dataset.

### Available Templates

| Template | Pattern | Description |
|----------|---------|-------------|
| Preview data | `SELECT * FROM {table} LIMIT 10` | Quick look at sample rows |
| Row count | `SELECT COUNT(*) FROM {table}` | Total row count |
| Filter by column | `SELECT * FROM {table} WHERE {column} = '?'` | Filter with placeholder |
| Group by | `SELECT {column}, COUNT(*) FROM {table} GROUP BY {column}` | Aggregate by column |
| Unique values | `SELECT DISTINCT {column} FROM {table}` | List distinct values |
| Table schema | `DESCRIBE {table}` | Show table structure |

### Using the Template Picker

| Key | Action |
|-----|--------|
| `T` | Open template picker |
| `↑` / `↓` | Navigate templates |
| `Enter` | Select and insert template |
| `Esc` | Close without inserting |

### Placeholder Resolution

Templates contain placeholders that are automatically resolved:

| Placeholder | Replaced With | Example |
|-------------|---------------|---------|
| `{table}` | Selected dataset as `namespace.name` | `ethereum.blocks` |
| `{column}` | First column from the dataset schema | `block_number` |

**Example workflow:**
1. Select `ethereum.transactions` in the sidebar
2. Press `Q` to enter query mode
3. Press `T` to open template picker
4. Select "Group by" template
5. The query is inserted as: `SELECT gas_used, COUNT(*) FROM ethereum.transactions GROUP BY gas_used`

The picker shows a **live preview** of the resolved SQL for each template, so you can see exactly what will be inserted before selecting.

## Favorite Queries

Save frequently-used queries as favorites:

- Press `*` or `F` to toggle the current query as a favorite
- Press `Ctrl+F` to open the favorites panel
- Press `d` in the favorites panel to delete a favorite

Favorites are persisted to `~/.config/ampcc/favorites.json`.

## Query Results

Results are displayed in a scrollable table below the query input:

| Key | Action |
|-----|--------|
| `j` / `↓` | Scroll down |
| `k` / `↑` | Scroll up |
| `Ctrl+D` | Page down |
| `Ctrl+U` | Page up |
| `E` | Export results to CSV |
| `s` + `1-9` | Sort by column number |
| `S` | Clear sort (show original order) |
| `Q` | Edit query |

### Exporting Results

Press `E` to export the current results to a CSV file. The file is saved to the current directory with a timestamped filename like `query_results_20260119_143052.csv`.

### Sorting Results

Column headers display numbers like `[1] column_name`, `[2] column_name` to identify sortable columns.

**How to sort:**

1. Press `s` to enter sort mode (footer shows "Press 1-9 for column")
2. Press a digit `1-9` to sort by that column
3. Press the same column again to toggle between ascending (▲) and descending (▼)
4. Press `S` to clear sorting and restore original order

**Sort indicators:**

- Column headers show `▲` for ascending sort, `▼` for descending
- The title bar shows the current sort state: `"sorted by column_name ▲"`

**Sort behavior:**

- Numeric columns are sorted numerically (e.g., 2 < 10, not "10" < "2")
- Text columns are sorted alphabetically (case-sensitive)
- NULL and empty values are sorted to the end
- Sorting is performed client-side on the displayed results

### Column Sizing

Columns are automatically sized based on their content:

- Width is calculated from header names and data values
- Minimum column width: 5 characters
- Maximum column width: 50 characters
- When columns exceed available space, they scale proportionally
- Narrow columns (IDs, booleans) get less space
- Wider columns (hashes, descriptions) get more space

## Tips

- Use `Tab` to cycle between Query input and Query results panes
- Long cell values are truncated in the display but exported in full
- NULL values sort to the end when sorting columns
- The title bar shows the current line count and history position
- SQL keywords are highlighted regardless of case (`select`, `SELECT`, `Select` all work)
