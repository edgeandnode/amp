---
name: "code-pattern"
description: "Structure template for `docs/code/pattern-*.md` guideline docs. Load when creating or editing pattern docs in docs/code/"
type: meta
scope: "global"
---

# Pattern Code Guideline Template

**MANDATORY structure for ALL `docs/code/pattern-*.md` documents**

## Structure

Every pattern guideline document contains the following sections in order.

### Frontmatter (required)

| Field | Value | Notes |
|-------|-------|-------|
| `name` | `pattern-<name-kebab-case>` | Matches filename minus `.md` |
| `description` | Discovery-optimized summary | No trailing period |
| `type` | `"core"` | Always |
| `scope` | `"global"` | Always |

See [code.md §2](code.md#2-frontmatter-requirements) for field rules and description guidelines.
See [code.md §3](code.md#3-naming-schema) for full naming rules.

### Header

#### Title (required)

H1 pattern name, optionally with a parenthetical clarification, e.g. "Typestate Pattern (State Machines with Types)".

#### Applicability (required)

Always the bold line `**MANDATORY for ALL Rust code in the workspace**`.

### Body

#### Rule (required)

Clear, actionable statement of what the pattern solves and when to apply it.
Includes guidance on how to recognize situations where the pattern is needed.

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
// Bad — {{why this doesn't use the pattern}}
{{bad_code}}
\`\`\`

\`\`\`rust
// Good — {{why this applies the pattern correctly}}
{{good_code}}
\`\`\`
```

**Multiple examples:**

```markdown
1. **{{example title}}**
{{description and context}}

\`\`\`rust
// Bad — {{why this doesn't use the pattern}}
{{bad_code}}
\`\`\`

\`\`\`rust
// Good — {{why this applies the pattern correctly}}
{{good_code}}
\`\`\`

2. **{{example title}}**
{{description and context}}

\`\`\`rust
// Bad — {{why this doesn't use the pattern}}
{{bad_code}}
\`\`\`

\`\`\`rust
// Good — {{why this applies the pattern correctly}}
{{good_code}}
\`\`\`
```

#### Why It Matters (required)

Practical consequences of not using the pattern and benefits of applying it.
Focuses on type safety, compile-time guarantees, maintainability, and correctness.

#### Pragmatism Caveat (required)

When the pattern is overkill and simpler alternatives suffice.
Every pattern must acknowledge when not to use it.

#### Checklist (required)

Code review verification items specific to the pattern.
Each item is a concrete check to confirm the pattern is applied correctly.

### Footer

#### References (optional)

Cross-references to related guideline docs within the project.
Pattern docs may link to principle docs as `Foundation` and to other pattern docs as `Related`.
See [code.md §4](code.md#4-cross-reference-rules) for relationship types and direction rules.

#### External References (optional)

Links to external articles, books, or resources that explain the pattern in depth.
Not project-internal.

## Template

Every pattern guideline document MUST follow this template:

```markdown
---
name: "pattern-<name-kebab-case>"
description: "{{Brief summary. Load when [trigger conditions], no period}}"
type: "core"
scope: "global"
---

# {{Pattern Title}} [({{Optional Parenthetical Clarification}})]

**MANDATORY for ALL Rust code in the workspace**

## Rule

{{Actionable rule. See Structure > Rule.}}

## Examples

{{Bad/Good code pairs. See Structure > Examples.}}

## Why It Matters

{{Practical consequences. See Structure > Why It Matters.}}

## Pragmatism Caveat

{{When the pattern is overkill. See Structure > Pragmatism Caveat.}}

## Checklist

- [ ] {{Verification item 1}}
- [ ] {{Verification item 2}}
- [ ] {{Verification item 3}}
- [ ] {{Verification item 4}}

## References

- [{{principle-name}}]({{principle-name}}.md) - Foundation: {{Brief description}}
- [{{related-pattern-name}}]({{related-pattern-name}}.md) - Related: {{Brief description}}

## External References

- [{{External reference title}}]({{url}})
```

## References

- [code](code.md) - Extends: Base code guideline documentation format specification
