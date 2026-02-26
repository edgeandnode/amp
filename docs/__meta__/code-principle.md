---
name: "code-principle"
description: "Structure template for `docs/code/principle-*.md` guideline docs. Load when creating or editing principle docs in docs/code/"
type: meta
scope: "global"
---

# Principle Code Guideline Template

**MANDATORY structure for ALL `docs/code/principle-*.md` documents**

## Structure

Every principle guideline document contains the following sections in order.

### Frontmatter (required)

| Field | Value | Notes |
|-------|-------|-------|
| `name` | `principle-<name-kebab-case>` | Matches filename minus `.md` |
| `description` | Discovery-optimized summary | No trailing period |
| `type` | `"principle"` | Always |
| `scope` | `"global"` | Always |

See [code.md §2](code.md#2-frontmatter-requirements) for field rules and description guidelines.
See [code.md §3](code.md#3-naming-schema) for full naming rules.

### Header

#### Title (required)

H1 principle name with a parenthetical clarification, e.g. "Law of Demeter (Principle of Least Knowledge)".

#### Applicability (required)

Always the bold line `**MANDATORY for ALL code in the workspace**`.

### Body

#### Rule (required)

Clear, actionable statement of what to do and what not to do.
Includes guidance on how to recognize violations.

#### Examples (required)

Bad/Good code pairs (at least one, recommended not more than 5) in Rust.
Each example has a brief description providing context, followed by the Bad/Good code pair.
When more than one example is provided, present them as a numbered list.
Always show Bad first, then Good.
Use comments to explain why each is bad or good.

**Note:** The templates below use `\`` to represent backticks.
Do not escape backticks in the actual document — use literal code block fences.

**Single example:**

```markdown
{{description and context}}

\`\`\`rust
// Bad — {{why this violates the principle}}
{{bad_code}}
\`\`\`

\`\`\`rust
// Good — {{why this follows the principle}}
{{good_code}}
\`\`\`
```

**Multiple examples:**

```markdown
1. **{{example title}}**
{{description and context}}

\`\`\`rust
// Bad — {{why this violates the principle}}
{{bad_code}}
\`\`\`

\`\`\`rust
// Good — {{why this follows the principle}}
{{good_code}}
\`\`\`

2. **{{example title}}**
{{description and context}}

\`\`\`rust
// Bad — {{why this violates the principle}}
{{bad_code}}
\`\`\`

\`\`\`rust
// Good — {{why this follows the principle}}
{{good_code}}
\`\`\`
```

#### Why It Matters (required)

Practical consequences of violating the principle and benefits of following it.
Focuses on maintenance burden, coupling, bug risk, and testability.

#### Pragmatism Caveat (required)

When deviation from the principle is acceptable and what documentation is required.
Every principle must acknowledge pragmatic exceptions.
Must state that undocumented violations are always wrong.

#### Checklist (required)

Code review verification items specific to the principle.
Each item is a concrete check to confirm the principle is followed.

### Footer

#### References (optional)

Cross-references to related principle docs within the project.
Principle docs may link to other principle docs as `Related`.
See [code.md §4](code.md#4-cross-reference-rules) for relationship types and direction rules.

#### External References (optional)

Links to external articles, books, or resources that explain the principle in depth.
Not project-internal.

## Template

Every principle guideline document MUST follow this template:

```markdown
---
name: "principle-<name-kebab-case>"
description: "{{Brief summary. Load when [trigger conditions], no period}}"
type: "principle"
scope: "global"
---

# {{Principle Title}} ({{Parenthetical Clarification}})

**MANDATORY for ALL code in the workspace**

## Rule

{{Actionable rule. See Structure > Rule.}}

## Examples

{{Bad/Good code pairs. See Structure > Examples.}}

## Why It Matters

{{Practical consequences. See Structure > Why It Matters.}}

## Pragmatism Caveat

{{When deviation is acceptable. See Structure > Pragmatism Caveat.}}

## Checklist

- [ ] {{Verification item 1}}
- [ ] {{Verification item 2}}
- [ ] {{Verification item 3}}
- [ ] {{Verification item 4}}

## References

- [{{related-principle-name}}]({{related-principle-name}}.md) - Related: {{Brief description}}

## External References

- [{{External reference title}}]({{url}})
```

## References

- [code](code.md) - Extends: Base code guideline documentation format specification

