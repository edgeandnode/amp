---
name: feature-status
description: Report feature maturity and status. Use when asking about project status, feature readiness, or which features are stable/experimental/development
allowed-tools: Bash(python3 .claude/skills/feature-status/report.py )
---

# Feature Status Skill

This skill generates status reports for Project Amp features based on their maturity level. It reads the `status` field from feature documentation frontmatter and organizes features by maturity.

## When to Use This Skill

Use this skill when:
- User asks "What's the project status?"
- User asks "What features are stable/experimental/development?"
- User wants to know "What features are ready for production?"
- User asks "Which features are mature vs experimental?"
- User requests "Show feature maturity levels"
- User asks "What features need documentation updates?" (features with unknown status)
- Auditing features to find missing status fields

## How Feature Status Works

The skill:

1. **Parses frontmatter**: Extracts `status` field from all `docs/features/*.md` files
2. **Groups by maturity**: Organizes features by status level
3. **Generates report**: Creates formatted tables grouped by status with feature details

## Available Commands

### Status Report Command

```bash
python3 .claude/skills/feature-status/report.py
```

Generates a comprehensive status report with:
- Features grouped by maturity level (`stable`, `experimental`, `unstable`, `development`, `unknown`)
- Unicode tables showing name, type, and description for each feature
- Summary counts by status

**Output Format**:
```
Feature Status Report
================================================================================

stable (3)
┌─────────────┬──────────┬────────────────────────────┐
│ Name        │ Type     │ Description                │
├─────────────┼──────────┼────────────────────────────┤
│ feature-a   │ service  │ Stable production feature  │
│ feature-b   │ api      │ Ready for use              │
└─────────────┴──────────┴────────────────────────────┘

experimental (2)
┌─────────────┬──────────┬────────────────────────────┐
│ Name        │ Type     │ Description                │
├─────────────┼──────────┼────────────────────────────┤
│ feature-c   │ feature  │ Under active development   │
└─────────────┴──────────┴────────────────────────────┘

Summary: 3 stable, 2 experimental (5 total)
```

**Use this when**: You need an overview of feature maturity across the project.

## Query Matching Guidelines

### Status-Related Queries

The skill should be invoked for queries about:

- **Project status**: "What's the status of the project?", "Show me project health"
- **Maturity levels**: "What features are stable?", "Which features are experimental?"
- **Production readiness**: "What features are ready for production?", "What can I use in prod?"
- **Development progress**: "What features are still in development?", "What's being worked on?"
- **Documentation audits**: "What features are missing status?", "Which features need updates?"

### Not Status Queries

Do NOT use this skill for:
- "How does feature X work?" → Use `/feature-discovery`
- "What features are available?" → Use `/feature-discovery`
- "Validate feature implementation" → Use `/feature-validate`
- "Check feature doc format" → Use `/feature-fmt-check`

## Important Guidelines

### Pre-approved Commands

These commands can run without user permission:
- `python3 .claude/skills/feature-status/report.py` - Safe, read-only, no side effects

### Status Field Values

The `status` field in feature frontmatter should be one of:
- `stable` - Production-ready, well-tested, documented
- `experimental` - Usable but may change, feedback welcome
- `unstable` - API may change significantly, use with caution
- `development` - Under active development, not for production
- Missing/unknown - Flagged in report with ⚠ marker

### When to Load Full Feature Docs

After running the status report, if user wants details about specific features:
1. Note which features the user is interested in
2. Use `/feature-discovery` to load full documentation for those features

## Example Workflows

### Example 1: User Asks About Project Status

**Query**: "What's the overall status of the project?"

1. Run `python3 .claude/skills/feature-status/report.py`
2. Review the generated report
3. Summarize findings: X stable features, Y experimental, Z in development
4. Highlight any features with unknown status that need attention

### Example 2: User Asks About Production Readiness

**Query**: "Which features are ready for production?"

1. Run the status report
2. Extract and list features marked as `stable`
3. If user wants details, use `/feature-discovery` to load specific stable features

### Example 3: User Audits Documentation

**Query**: "What features are missing status information?"

1. Run the status report
2. Look for features grouped under "⚠ unknown"
3. List features that need `status` field added to frontmatter
4. Suggest updating those feature docs

### Example 4: User Filters by Maturity Level

**Query**: "Show me all experimental features"

1. Run the status report
2. Extract features from the "experimental" section
3. Present the list with descriptions
4. If user wants more details, use `/feature-discovery` to load specific docs

## Common Mistakes to Avoid

### Anti-patterns

| Mistake | Why It's Wrong | Do This Instead |
|---------|----------------|-----------------|
| Manually list features by status | List becomes stale | Always run report.py |
| Guess status from feature name | Inaccurate | Read actual `status` field |
| Skip unknown status features | Missing metadata | Highlight for updates |
| Load all feature docs for status | Bloats context | Use report for overview only |

### Best Practices

- Run the report script to get current status
- Use report for overview, `/feature-discovery` for details
- Flag features with unknown status for documentation updates
- Present status grouped by maturity level for clarity

## Next Steps

After generating a status report:

1. **Present findings** - Summarize status distribution to user
2. **Identify actions** - Note features needing status updates
3. **Provide details** - Use `/feature-discovery` if user wants specifics
4. **Suggest improvements** - Recommend adding status to unknown features
