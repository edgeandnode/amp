---
name: "udf-documentation"
description: "UDF documentation patterns for common crate. Load when documenting UDFs in common crate"
type: crate
scope: "crate:common"
---

📐 UDF Documentation Guidelines
================================

This document provides the **mandatory** documentation template for DataFusion User-Defined Functions (UDFs) in the `common` crate.

## Context and Purpose

**🎯 PRIMARY PURPOSE: Consistent UDF Documentation**

All UDFs in `crates/core/common/src/evm/udfs/` must be documented using this template. Consistent documentation ensures:
- SQL users can quickly understand function behavior
- Error conditions are explicit and predictable
- Code is self-documenting and maintainable

**📍 Where to Apply:**
- UDF struct definitions (the main `pub struct UdfName` block)
- Located in `crates/core/common/src/evm/udfs/*.rs`

## Documentation Template

🚨 **MANDATORY**: Every UDF struct MUST include comprehensive documentation following this EXACT pattern:

```rust
/// DataFusion UDF that [brief action description in present tense].
///
/// [Extended description explaining:
///  - What the function does in practical terms
///  - Behavior variations (e.g., positive vs negative values)
///  - Common use cases or conversion directions]
///
/// # SQL Usage
///
/// ```ignore
/// // [Comment describing what this example demonstrates]
/// function_name('arg1', arg2)  // Returns "expected_result"
///
/// // [Another example with different inputs]
/// function_name('different_args', other_arg)  // Returns "different_result"
///
/// // [Edge case or advanced usage example]
/// function_name('edge_case')  // Returns "edge_result"
/// ```
///
/// # Arguments
///
/// * `param_name` - [DataType] description (e.g., valid formats, constraints)
/// * `param_name2` - [DataType] description
///
/// # Returns
///
/// [Description of return type, format, and any transformations applied]
///
/// # Errors
///
/// Returns a planning error if:
/// - [Specific error condition 1]
/// - [Specific error condition 2]
/// - Input is null
///
/// # Nullability
///
/// Returns null if input is null.
```

## Section-by-Section Guide

### 1. 📌 Title Line (MANDATORY)

**Format:** `DataFusion UDF that [action in present tense].`

**Requirements:**
- Start with "DataFusion UDF that"
- Use present tense verbs (converts, shifts, decodes, encodes)
- Be concise but specific

**Examples:**
```rust
/// DataFusion UDF that converts `FixedSizeBinary(20)` or `FixedSizeBinary(32)` to hex string.
/// DataFusion UDF that shifts the decimal point of a numeric value.
/// DataFusion UDF that decodes EVM event logs into structured data.
```

### 2. 📄 Extended Description (MANDATORY)

**Purpose:** Explain behavior that isn't obvious from the title.

**Include:**
- Practical explanation of what the function does
- Behavior variations based on input (e.g., positive vs negative values)
- Common use cases or conversion directions

**Example:**
```rust
/// This function shifts the decimal point by the specified number of places:
/// - **Positive units**: Shifts right (multiplies by 10^units) - converts human-readable to raw
/// - **Negative units**: Shifts left (divides by 10^units) - converts raw to human-readable
```

### 3. 💻 SQL Usage Section (MANDATORY)

**Format:** `# SQL Usage` with code block using `ignore` attribute

**Requirements:**
- Use ```` ```ignore ```` 
- Include 2-4 practical examples
- Add comments explaining each example
- Show expected return value as inline comment

**Example:**
```rust
/// # SQL Usage
///
/// ```ignore
/// // Convert 1.5 ETH to wei (shift right by 18 places)
/// shift_units('1.5', 18)  // Returns "1500000000000000000"
///
/// // Convert wei back to ETH (shift left by 18 places)
/// shift_units('1500000000000000000', -18)  // Returns "1.5"
///
/// // Convert 100 USDC to raw units (6 decimals)
/// shift_units('100', 6)  // Returns "100000000"
/// ```
```

### 4. 📋 Arguments Section (MANDATORY)

**Format:** `# Arguments` with bullet list

**Requirements:**
- List each parameter with name, type, and description
- Include valid value ranges or formats
- Mention constraints (e.g., "must be positive", "with or without 0x prefix")

**Example:**
```rust
/// # Arguments
///
/// * `value` - Decimal/Integer string (e.g., "1.5", "100", "0.001")
/// * `units` - Number of decimal places to shift (positive = right, negative = left)
```

**Another Example:**
```rust
/// # Arguments
///
/// * `hex_string` - Hex string (40 or 64 characters), with or without `0x` prefix
```

### 5. 📤 Returns Section (CONDITIONAL)

**Format:** `# Returns`

**When to Include:**
- When return value needs clarification beyond the title
- When format transformations occur (e.g., "without trailing zeros")
- When output type varies based on input

**Example:**
```rust
/// # Returns
///
/// A string representation of the result without trailing zeros.
```

**Skip When:** The return type is obvious from the title (e.g., "converts X to Y").

### 6. ⚠️ Errors Section (MANDATORY)

**Format:** `# Errors` with "Returns a planning error if:" followed by bullet list

**Requirements:**
- List ALL conditions that cause errors
- Be specific about what makes input invalid
- Always include null handling if applicable

**Example:**
```rust
/// # Errors
///
/// Returns a planning error if:
/// - Value is not a valid decimal/integer string
/// - Units is null
/// - Input is null
```

**Another Example:**
```rust
/// # Errors
///
/// Returns a planning error if:
/// - Input is not a valid hex string
/// - Decoded bytes are not exactly 20 or 32 bytes
/// - Input is null
```

### 7. 🔄 Nullability Section (CONDITIONAL)

**Format:** `# Nullability`

**When to Include:**
- When null handling is not covered in Errors section
- When the UDF has specific null-propagation behavior

**Example:**
```rust
/// # Nullability
///
/// Returns null if input is null.
```

**Skip When:** Null behavior is already documented in the Errors section.

## Checklist for New UDFs

Before submitting a new UDF, verify:

- [ ] Title line starts with "DataFusion UDF that"
- [ ] Extended description explains practical behavior
- [ ] SQL Usage section has 2-4 practical examples
- [ ] Arguments section lists all parameters with types
- [ ] Errors section lists all error conditions
- [ ] Returns section included if return format needs clarification
- [ ] Nullability section included if not covered in Errors

---

🚨 **CRITICAL**: This guide MUST be referenced when implementing new UDFs or updating existing ones to ensure consistency with established patterns in the `common` crate.
