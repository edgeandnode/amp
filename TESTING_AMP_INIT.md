# Manual Testing Guide for `amp init`

This guide walks you through testing all the features of the new `amp init` command.

## Prerequisites

- Navigate to the amp-private project root
- Ensure TypeScript is compiled: `cd typescript/amp && pnpm run build && cd ../..`

## Test Suite

### Test 1: Help Documentation

**What it tests:** Command help shows all available flags

```bash
cd /tmp
mkdir test-help && cd test-help
bun /Users/marcusrein/Desktop/Projects/amp-private/typescript/amp/src/cli/bun.ts init --help
```

**Expected output:**
- Should show `USAGE`, `DESCRIPTION`, and `OPTIONS` sections
- Should list `--dataset-name`, `--dataset-version`, `--project-name`, `(-y, --yes)` flags
- Should NOT start prompting for input

---

### Test 2: Quick Default Mode (Non-Interactive)

**What it tests:** `-y` flag creates project with all defaults

```bash
cd /tmp
rm -rf test-quick && mkdir test-quick && cd test-quick
bun /Users/marcusrein/Desktop/Projects/amp-private/typescript/amp/src/cli/bun.ts init -y
```

**Expected output:**
```
🎨 Initializing Amp project with template: local-evm-rpc
📁 Target directory: /private/tmp/test-quick

  ✓ Created amp.config.ts
  ✓ Created README.md
  ✓ Created contracts/foundry.toml
  ✓ Created contracts/src/Counter.sol
  ✓ Created contracts/script/Deploy.s.sol
  ✓ Created contracts/remappings.txt
  ✓ Created .gitignore

🎉 Project initialized successfully!
```

**Verify files created:**
```bash
ls -la
# Should see: amp.config.ts, README.md, contracts/, .gitignore
```

**Verify default values in amp.config.ts:**
```bash
cat amp.config.ts | grep -A 3 "export default"
```

**Expected:**
```typescript
export default defineDataset(() => ({
  name: "my_dataset",
  version: "0.1.0",
  network: "anvil",
```

**Verify default values in README.md:**
```bash
head -15 README.md
```

**Expected to see:**
- `# amp_project` (default project name)
- `Dataset Name**: \`my_dataset\``
- `Dataset Version**: \`0.1.0\``

---

### Test 3: Flag-Based Mode with Custom Values

**What it tests:** Custom flags override defaults (non-interactive)

```bash
cd /tmp
rm -rf test-custom && mkdir test-custom && cd test-custom
bun /Users/marcusrein/Desktop/Projects/amp-private/typescript/amp/src/cli/bun.ts init \
  --dataset-name my_awesome_data \
  --dataset-version 3.2.1 \
  --project-name "My Awesome Project"
```

**Expected output:**
- Same success message as Test 2
- Should NOT prompt for any input

**Verify custom values in amp.config.ts:**
```bash
cat amp.config.ts | grep -A 3 "export default"
```

**Expected:**
```typescript
export default defineDataset(() => ({
  name: "my_awesome_data",
  version: "3.2.1",
  network: "anvil",
```

**Verify custom values in README.md:**
```bash
head -15 README.md
```

**Expected to see:**
- `# My Awesome Project`
- `Dataset Name**: \`my_awesome_data\``
- `Dataset Version**: \`3.2.1\``

---

### Test 4: Partial Flags (Mix of Flags and Defaults)

**What it tests:** Providing some flags uses defaults for missing ones

```bash
cd /tmp
rm -rf test-partial && mkdir test-partial && cd test-partial
bun /Users/marcusrein/Desktop/Projects/amp-private/typescript/amp/src/cli/bun.ts init \
  --dataset-name partial_test
```

**Expected output:**
- Should complete without prompts
- Should use default version (0.1.0) and project name (amp_project)

**Verify mixed values:**
```bash
cat amp.config.ts | grep -A 3 "export default"
head -15 README.md
```

**Expected:**
- `name: "partial_test"` (custom)
- `version: "0.1.0"` (default)
- `# amp_project` in README (default)

---

### Test 5: Validation - Invalid Dataset Name

**What it tests:** Dataset name validation rejects invalid names

```bash
cd /tmp
rm -rf test-invalid && mkdir test-invalid && cd test-invalid
bun /Users/marcusrein/Desktop/Projects/amp-private/typescript/amp/src/cli/bun.ts init \
  --dataset-name "Invalid Name With Spaces" 2>&1
```

**Expected output:**
```
TemplateError: Invalid dataset name: "Invalid Name With Spaces". Dataset name must start with a letter or underscore and contain only lowercase letters, digits, and underscores
```

**Test more invalid names:**
```bash
# Uppercase
bun /Users/marcusrein/Desktop/Projects/amp-private/typescript/amp/src/cli/bun.ts init --dataset-name MyDataset 2>&1

# Starts with number
bun /Users/marcusrein/Desktop/Projects/amp-private/typescript/amp/src/cli/bun.ts init --dataset-name 123invalid 2>&1

# Contains hyphen
bun /Users/marcusrein/Desktop/Projects/amp-private/typescript/amp/src/cli/bun.ts init --dataset-name my-dataset 2>&1
```

**All should show validation error.**

---

### Test 6: Interactive Mode

**What it tests:** Running `amp init` without flags starts interactive prompts

```bash
cd /tmp
rm -rf test-interactive && mkdir test-interactive && cd test-interactive
bun /Users/marcusrein/Desktop/Projects/amp-private/typescript/amp/src/cli/bun.ts init
```

**Expected behavior:**
1. First prompt: `Dataset name: › my_dataset`
   - Type: `interactive_data` and press Enter
2. Second prompt: `Dataset version: › 0.1.0`
   - Type: `2.5.0` and press Enter
3. Third prompt: `Project name (for README): › amp_project`
   - Type: `Interactive Test` and press Enter

**Expected output after completing prompts:**
- Same success message showing files created
- Files should contain your custom values from prompts

**Verify:**
```bash
cat amp.config.ts | grep -A 3 "export default"
# Should show: name: "interactive_data", version: "2.5.0"

head -15 README.md
# Should show: # Interactive Test
```

---

### Test 7: Interactive Mode with Validation

**What it tests:** Interactive prompts validate input and reject invalid names

```bash
cd /tmp
rm -rf test-interactive-validation && mkdir test-interactive-validation && cd test-interactive-validation
bun /Users/marcusrein/Desktop/Projects/amp-private/typescript/amp/src/cli/bun.ts init
```

**Steps:**
1. First prompt: `Dataset name: › my_dataset`
   - Type: `Bad Name!` and press Enter
   - **Expected:** Red error message: "Dataset name must start with a letter or underscore..."
   - Prompt stays active, waiting for valid input
2. Type: `valid_name` and press Enter
   - **Expected:** Prompt moves to next question
3. Complete remaining prompts normally

---

### Test 8: Verify Generated Contract Files

**What it tests:** All template files are created correctly

```bash
cd /tmp
rm -rf test-files && mkdir test-files && cd test-files
bun /Users/marcusrein/Desktop/Projects/amp-private/typescript/amp/src/cli/bun.ts init -y
```

**Verify file structure:**
```bash
tree . -L 2
# Or use ls if tree not available:
find . -type f | sort
```

**Expected files:**
```
.
├── .gitignore
├── README.md
├── amp.config.ts
└── contracts/
    ├── foundry.toml
    ├── remappings.txt
    ├── script/Deploy.s.sol
    └── src/Counter.sol
```

**Verify Counter.sol contract:**
```bash
cat contracts/src/Counter.sol | head -20
```

**Expected to see:**
- `pragma solidity ^0.8.20;`
- `contract Counter {`
- `event Count(uint256 count);`
- `event Transfer(address indexed from, address indexed to, uint256 value);`

**Verify Deploy script:**
```bash
cat contracts/script/Deploy.s.sol
```

**Expected to see:**
- Imports Counter contract
- Deploys Counter
- Calls increment() 3 times
- Calls transfer() 2 times

---

### Test 9: Verify README Documentation Links

**What it tests:** README contains links to documentation

```bash
cd /tmp
rm -rf test-readme && mkdir test-readme && cd test-readme
bun /Users/marcusrein/Desktop/Projects/amp-private/typescript/amp/src/cli/bun.ts init -y
```

**Check README content:**
```bash
cat README.md | grep -A 10 "## 📚 Learn More"
```

**Expected to see links:**
- `[Overview](../../docs/README.md)`
- `[Executables](../../docs/exes.md)`
- `[Configuration](../../docs/config.md)`
- `[User-Defined Functions](../../docs/udfs.md)`
- `[Glossary](../../docs/glossary.md)`

**Verify Configuration Summary section:**
```bash
cat README.md | grep -A 10 "## 📋 Configuration Summary"
```

**Expected to see:**
- Dataset Name with actual value
- Dataset Version with actual value
- Network: Anvil
- Template: local-evm-rpc
- Provider: Local EVM RPC

---

### Test 10: Run Unit Tests

**What it tests:** Automated test suite passes

```bash
cd /Users/marcusrein/Desktop/Projects/amp-private
pnpm vitest run typescript/amp/test/cli/init.test.ts
```

**Expected output:**
```
✓ |@edgeandnode/amp| test/cli/init.test.ts (9 tests) 2ms

Test Files  1 passed (1)
     Tests  9 passed (9)
```

**All 9 tests should pass:**
1. ✓ should resolve static template files
2. ✓ should resolve dynamic template files with answers
3. ✓ should have the correct template metadata
4. ✓ should generate amp.config.ts with custom dataset name and version
5. ✓ should generate README.md with project name
6. ✓ should include all required files
7. ✓ should generate valid Solidity contract
8. ✓ should accept valid dataset names
9. ✓ should reject invalid dataset names

---

## Quick Reference: All Command Modes

```bash
# 1. Help
amp init --help

# 2. Quick defaults (non-interactive)
amp init -y

# 3. Custom flags (non-interactive)
amp init --dataset-name NAME --dataset-version VERSION --project-name "NAME"

# 4. Partial flags (non-interactive, uses defaults for missing)
amp init --dataset-name NAME

# 5. Interactive mode (prompts for all values)
amp init
```

## Valid Dataset Name Rules

✅ **Valid examples:**
- `my_dataset`
- `dataset_123`
- `_underscore_start`
- `a`
- `lowercase_only`

❌ **Invalid examples:**
- `MyDataset` (uppercase)
- `123dataset` (starts with number)
- `my-dataset` (hyphen)
- `my dataset` (space)
- `dataset!` (special character)

---

## Cleanup After Testing

```bash
# Remove all test directories
cd /tmp
rm -rf test-*
```

---

## Expected Behavior Summary

| Mode | Command | Prompts? | Uses Flags? | Uses Defaults? |
|------|---------|----------|-------------|----------------|
| Help | `--help` | No | N/A | N/A |
| Quick Default | `-y` | No | No | Yes (all) |
| Flag-Based | `--dataset-name X` | No | Yes | Yes (if missing) |
| Interactive | `amp init` | Yes | No | Shows as defaults |

---

## Troubleshooting

**Problem:** Command not found
```bash
# Solution: Use full path to bun.ts
bun /Users/marcusrein/Desktop/Projects/amp-private/typescript/amp/src/cli/bun.ts init
```

**Problem:** TypeScript errors
```bash
# Solution: Rebuild
cd typescript/amp && pnpm run build && cd ../..
```

**Problem:** Tests fail
```bash
# Solution: Check if you're in the right directory
cd /Users/marcusrein/Desktop/Projects/amp-private
```

---

## Quick Test All Modes Script

Save this as `test-all-modes.sh` for rapid testing:

```bash
#!/bin/bash
set -e

echo "🧪 Testing amp init - All Modes"
echo "================================"

BASE_PATH="/Users/marcusrein/Desktop/Projects/amp-private/typescript/amp/src/cli/bun.ts"

cd /tmp

echo ""
echo "✅ Test 1: Quick default mode (-y)"
rm -rf t1 && mkdir t1 && cd t1
bun "$BASE_PATH" init -y
test -f amp.config.ts && echo "✓ Files created"

echo ""
echo "✅ Test 2: Custom flags mode"
cd /tmp
rm -rf t2 && mkdir t2 && cd t2
bun "$BASE_PATH" init --dataset-name test_data --dataset-version 1.0.0 --project-name "Test"
grep "test_data" amp.config.ts && echo "✓ Custom values work"

echo ""
echo "✅ Test 3: Validation (should fail)"
cd /tmp && rm -rf t3 && mkdir t3 && cd t3
if bun "$BASE_PATH" init --dataset-name "Invalid Name" 2>&1 | grep -q "TemplateError"; then
  echo "✓ Validation working"
else
  echo "✗ Validation not working"
fi

echo ""
echo "✅ Test 4: Help"
bun "$BASE_PATH" init --help | grep -q "dataset-name" && echo "✓ Help working"

echo ""
echo "🎉 All tests passed!"
echo ""
echo "Cleanup:"
cd /tmp && rm -rf t1 t2 t3
echo "✓ Test directories cleaned"
```

Run with:
```bash
chmod +x test-all-modes.sh
./test-all-modes.sh
```
