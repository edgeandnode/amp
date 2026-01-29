#!/usr/bin/env python3
"""
Feature Status Report Generator

Parses YAML frontmatter from docs/features/*.md and generates a status report
grouped by maturity level (stable, experimental, unstable, development, unknown).
"""

import os
import sys
import re
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Tuple


def parse_frontmatter(content: str) -> Dict[str, str]:
    """Extract YAML frontmatter from markdown content."""
    match = re.match(r'^---\s*\n(.*?)\n---\s*\n', content, re.DOTALL)
    if not match:
        return {}

    frontmatter = {}
    for line in match.group(1).split('\n'):
        if ':' in line:
            key, value = line.split(':', 1)
            frontmatter[key.strip()] = value.strip().strip('"\'')

    return frontmatter


def get_terminal_width() -> int:
    """Get terminal width, or return None if not a TTY."""
    if not sys.stdout.isatty():
        return None

    try:
        import shutil
        return shutil.get_terminal_size().columns
    except:
        return 80


def truncate_text(text: str, max_width: int) -> str:
    """Truncate text with ellipsis if longer than max_width."""
    if len(text) <= max_width:
        return text
    return text[:max_width - 3] + '...'


def draw_table(headers: List[str], rows: List[List[str]], terminal_width: int = None) -> str:
    """Draw a unicode table with the given headers and rows."""
    if not rows:
        return ""

    # Calculate column widths
    num_cols = len(headers)
    col_widths = [len(h) for h in headers]

    for row in rows:
        for i, cell in enumerate(row):
            col_widths[i] = max(col_widths[i], len(cell))

    # If terminal width is specified and we're truncating, adjust description column
    if terminal_width:
        # Calculate minimum needed width: borders + padding
        # Format: "│ col1 │ col2 │ col3 │" = 4 borders + 6 spaces + content
        borders_and_padding = 4 + (num_cols * 2)
        available = terminal_width - borders_and_padding

        # Give description column (last) the remaining space
        fixed_width = sum(col_widths[:-1])
        if fixed_width + col_widths[-1] > available:
            col_widths[-1] = max(20, available - fixed_width)

    # Build table
    lines = []

    # Top border
    lines.append('┌' + '┬'.join('─' * (w + 2) for w in col_widths) + '┐')

    # Header row
    header_cells = [f" {headers[i]:<{col_widths[i]}} " for i in range(num_cols)]
    lines.append('│' + '│'.join(header_cells) + '│')

    # Header separator
    lines.append('├' + '┼'.join('─' * (w + 2) for w in col_widths) + '┤')

    # Data rows
    for row in rows:
        cells = []
        for i, cell in enumerate(row):
            truncated = truncate_text(cell, col_widths[i]) if terminal_width and i == num_cols - 1 else cell
            cells.append(f" {truncated:<{col_widths[i]}} ")
        lines.append('│' + '│'.join(cells) + '│')

    # Bottom border
    lines.append('└' + '┴'.join('─' * (w + 2) for w in col_widths) + '┘')

    return '\n'.join(lines)


def main():
    # Find docs/features directory relative to script location
    script_dir = Path(__file__).parent
    repo_root = script_dir.parent.parent.parent
    features_dir = repo_root / 'docs' / 'features'

    if not features_dir.exists():
        print(f"Error: Features directory not found at {features_dir}", file=sys.stderr)
        sys.exit(1)

    # Parse all feature docs
    features_by_status = defaultdict(list)

    for md_file in sorted(features_dir.glob('*.md')):
        try:
            content = md_file.read_text()
            frontmatter = parse_frontmatter(content)

            if not frontmatter:
                continue

            name = frontmatter.get('name', md_file.stem)
            doc_type = frontmatter.get('type', 'unknown')
            description = frontmatter.get('description', 'No description')
            status = frontmatter.get('status', None)

            # Group by status, or "unknown" if missing
            status_key = status if status else '⚠ unknown'
            features_by_status[status_key].append({
                'name': name,
                'type': doc_type,
                'description': description
            })
        except Exception as e:
            print(f"Warning: Failed to parse {md_file.name}: {e}", file=sys.stderr)

    if not features_by_status:
        print("No feature documents found.")
        return

    # Get terminal width for truncation (only if TTY)
    terminal_width = get_terminal_width()

    # Print report
    print("Feature Status Report")
    print("=" * (terminal_width if terminal_width else 80))
    print()

    # Define status order
    status_order = ['stable', 'experimental', 'unstable', 'development', '⚠ unknown']

    # Count features by status for summary
    counts = {}

    # Print each status group
    for status in status_order:
        if status not in features_by_status:
            continue

        features = sorted(features_by_status[status], key=lambda f: f['name'])
        counts[status] = len(features)

        # Print status header
        print(f"{status} ({len(features)})")

        # Prepare table data
        headers = ['Name', 'Type', 'Description']
        rows = [[f['name'], f['type'], f['description']] for f in features]

        # Draw table
        table = draw_table(headers, rows, terminal_width)
        print(table)
        print()

    # Print summary
    summary_parts = []
    for status in status_order:
        if status in counts:
            summary_parts.append(f"{counts[status]} {status}")

    total = sum(counts.values())
    summary = f"Summary: {', '.join(summary_parts)} ({total} total)"
    print(summary)


if __name__ == '__main__':
    main()
