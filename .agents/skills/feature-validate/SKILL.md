---
name: feature-validate
description: Verify feature doc alignment with implementation. Use when asking about feature implementation status, or to check that documented functionality exists in code and has test coverage
---

# Feature Validate Skill

This skill verifies that documented functionality is actually implemented in the codebase and that the implementation aligns with what's documented. It also checks for test coverage and warns about untested functionality.

## When to Use This Skill

Use this skill when:
- User asks about the status of a feature implementation
- User explicitly asks to verify a feature doc against implementation
- Auditing existing feature docs for accuracy
- Checking if documented functionality has test coverage
- Validating that code matches what's documented

## Verification Scope

Features can be implemented in many forms - not just functions. Verify ALL documented capabilities:

### Types of Documented Functionality

| Type | Examples | What to Verify |
|------|----------|----------------|
| **Functions/Methods** | UDFs, utility functions | Signature, args, return type |
| **HTTP Endpoints** | REST APIs, health checks | Method, path, request/response format |
| **Data Flows** | Request processing, ETL pipelines | Components involved, data transformations |
| **Protocols** | Arrow Flight, gRPC | Message types, connection handling |
| **Configuration** | Settings, environment vars | Config keys, defaults, validation |
| **Components** | Services, workers | Initialization, lifecycle, interactions |

### 1. Functionality Verification

Each capability described in the doc MUST exist in production code:
- Documented components must exist in the codebase
- Documented behaviors must match actual implementation
- Documented data flows must be traceable through components
- Documented constraints and limitations must be enforced

### 2. Production Code Review

- Locate implementation files from Architecture/File Locations section
- Verify documented components/endpoints/functions exist
- Check that documented behavior matches implementation
- Verify documented interactions between components are accurate

### 3. Test Coverage Review

- Search for unit tests covering documented functionality
- Search for E2E/integration tests covering documented scenarios
- Assess if existing tests cover happy path and critical corner cases
- Suggest specific test enhancements when gaps are found

### Test Coverage Philosophy: "Just the Right Amount"

Aim for sufficient coverage without over-testing:

**Must Have (HIGH priority)**:
- Happy path for each documented capability
- Error handling for documented failure modes
- Critical corner cases mentioned in Limitations section

**Should Have (MEDIUM priority)**:
- Edge cases for documented parameters (NULL, empty, boundary values)
- Integration points between documented components

**Not Required**:
- Exhaustive permutation testing
- Trivial variations of already-covered scenarios
- Implementation details not exposed in documentation

### 4. Misalignment Detection

- Flag documented features that don't exist in code
- Flag implemented features that behave differently than documented
- Flag missing test coverage for documented scenarios

## Verification Workflow

### Step 1: Parse Feature Doc

Extract from the feature doc:
- Documented capabilities from Usage section (functions, endpoints, behaviors)
- Component interactions from Architecture section
- Documented constraints and limitations
- File paths from Architecture section

### Step 2: Verify Production Code

1. Read implementation files listed in Architecture/File Locations
2. Match documented capabilities to actual implementations
3. For distributed features, trace the flow through multiple components
4. Check documented constraints are enforced

**What to verify by type**:

**For Functions/UDFs**:
- Does the function exist with documented signature?
- Do parameter types match?
- Does return type match?

**For HTTP Endpoints**:
- Does the endpoint exist at documented path?
- Does it accept documented HTTP method?
- Does request/response format match?

**For Data Flows**:
- Do documented components exist?
- Is the documented flow accurate?
- Are transformations implemented as described?

**For Configuration**:
- Do documented config keys exist?
- Are documented defaults accurate?
- Is validation implemented as described?

### Step 3: Review Test Coverage

Search the codebase for tests that exercise documented functionality:

1. **Unit Tests**: Search for component/function names in test modules
2. **Integration Tests**: Search for feature-related test cases
3. **E2E Tests**: Search for scenarios that exercise the documented flow

**Use Grep tool** to search for:
- Key identifiers documented in the feature
- Component names and interactions
- Test functions that reference the implementation

### Step 4: Generate Report

Produce a structured report listing:
- Verified functionality (docs match implementation)
- Misalignments (docs don't match implementation)
- Test coverage status per documented feature
- Warnings for untested functionality

## Report Format

```markdown
## Feature Implementation Verification: <filename>

### Functionality Verification

| Documented Capability | Implementation | Status |
|-----------------------|----------------|--------|
| HTTP endpoint POST /query | `jsonl.rs:handle_query()` | ✅ VERIFIED |
| Request flow: client → server → DataFusion | Multiple files | ✅ VERIFIED |
| Config option `jsonl_addr` | `config.rs:JsonlConfig` | ✅ VERIFIED |
| Returns NDJSON format | `jsonl.rs:encode_response()` | ✅ VERIFIED |
| Streaming queries rejected | NOT FOUND | ❌ MISSING |

### Test Coverage

| Documented Capability | Tests Found | Status |
|-----------------------|-------------|--------|
| Basic query execution | Yes | ✅ COVERED |
| Error response format | Yes | ✅ COVERED |
| Config validation | No | ⚠️ NO TESTS |

### Suggested Test Enhancements

⚠️ **Missing critical test coverage**:

| Priority | Scenario | Type | Rationale |
|----------|----------|------|-----------|
| ⚠️ HIGH | Config validation errors | Corner case | Documented but untested |
| ⚠️ HIGH | SQL parse error response | Happy path | Core error handling |
| MEDIUM | Empty query string | Corner case | Boundary condition |

### Misalignments Found

1. **MISSING IMPLEMENTATION**: Streaming query rejection not implemented
2. **BEHAVIOR DIFFERENCE**: Docs say "400 Bad Request" but code returns 500
3. **CONFIG MISMATCH**: Default port is 1604, not 1603 as documented

### Verdict: PASS/FAIL
- Misalignments found: X
- ⚠️ Test coverage warnings: Y (missing critical tests)
```

## Verification Guidelines

### What to Check in Production Code

Depends on the type of documented capability:

1. **Components & Services**
   - Does the component exist in the documented location?
   - Are documented lifecycle methods implemented?
   - Do component interactions match documentation?

2. **Endpoints & APIs**
   - Does the endpoint exist at documented path/method?
   - Does request/response format match?
   - Are documented status codes returned?

3. **Data Flows**
   - Can you trace the documented flow through code?
   - Are all documented transformations implemented?
   - Do components interact as described?

4. **Configuration**
   - Do documented config keys exist?
   - Are defaults accurate?
   - Is validation implemented?

### What to Check in Tests

Evaluate test coverage against the "Just the Right Amount" philosophy:

**For each documented capability, check:**
- Is there a happy path test? (HIGH priority if missing)
- Are documented error cases tested? (HIGH priority if missing)
- Are Limitations section corner cases tested? (HIGH priority if missing)
- Are parameter edge cases tested? (MEDIUM priority if missing)

**When suggesting enhancements:**
- Be specific about the scenario to test
- Classify as happy path or corner case
- Explain why it matters (e.g., "documented error handling", "boundary condition")

### Enhancement Priority Criteria

**⚠️ HIGH Priority** - Issue WARNING, suggest immediately:
- No happy path test for a documented capability
- Documented error handling has no test
- Critical corner case from Limitations section untested

**MEDIUM Priority** - Suggest as improvement:
- Edge cases for optional parameters
- Boundary value testing
- Integration between documented components

**LOW Priority** - Note but don't emphasize:
- Additional variations of covered scenarios
- Non-critical edge cases

**Note**: Missing HIGH priority tests should always trigger a ⚠️ WARNING in the report.

## Pre-approved Commands

These tools/commands can run without user permission:
- Read tool for feature docs and source files
- Grep tool for searching implementations and tests
- Glob tool for finding files

## Example Verification Sessions

### Example 1: UDF Feature (Function-focused)

**Scenario**: Verify `docs/features/udf-builtin-evm-eth-call.md`

1. **Parse doc** - Extract documented capabilities:
   - `eth_call(from, to, input, block)` function signature
   - Returns struct with `data` and `message` fields
   - Supports block tags: "latest", "pending", "earliest"

2. **Verify implementation** - Read files from Architecture section:
   - Verify function exists with correct signature
   - Verify return type matches
   - Verify block tag handling exists

3. **Search for tests** - Check coverage:
   - Search for "eth_call" in test files
   - Check which documented scenarios are tested

4. **Generate report** with findings

### Example 2: HTTP Endpoint Feature (Distributed)

**Scenario**: Verify `docs/features/query-jsonl-batch.md`

1. **Parse doc** - Extract documented capabilities:
   - POST endpoint on port 1603
   - Request flow: HTTP → SQL parsing → DataFusion → NDJSON response
   - Configuration: `jsonl_addr` setting
   - Error responses: specific error codes

2. **Verify implementation** - Trace through components:
   - Check HTTP handler exists
   - Verify SQL parsing matches docs
   - Check config structure matches
   - Verify error codes match

3. **Search for tests** - Check coverage:
   - Search for endpoint tests
   - Check error handling tests

4. **Generate report** with findings

## Next Steps

After verification:

1. **If misalignments found** - Either fix the docs or fix the implementation
2. **If test coverage warnings** - Consider adding tests or documenting the gap
3. **If all passes** - Feature doc is verified and ready for commit
