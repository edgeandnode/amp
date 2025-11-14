## What This Command Does

The `verify` command performs integrity checks over block data in a dataset.
You can choose exactly which parts to verify: header, transactions, receipts, or
header accumulators (external).

## Modes (internal vs external checks)

- header
  - Verifies dataset header integrity
  - Internal check (no external data required)
- transactions
  - Verifies transaction data consistency
  - Internal check (no external data required)
- receipts
  - Verifies receipt data correctness
  - Internal check (no external data required)
- header-accumulators
  - Verifies header accumulators via an external data path
  - External check: depends on data not available in nozzle
  - Requires an explicit path to the accumulators

## Flags

- `--mode <MODE>`
  - Values: `header`, `transactions`, `receipts`, `header-accumulators`
  - Default: `header` (if not specified)
- `--data-path <PATH>` (or `--accumulators-path <PATH>`)
  - Required only when `--mode header-accumulators`
  - Path to external data containing the header accumulators

## Behavior Summary

- Internal checks (header, transactions, receipts):
  - Run locally; no external data required
  - Exit non-zero on failure
- External check (header-accumulators):
  - Requires `--data-path` to run
  - If the path is missing/invalid, the command reports an error

## Examples

- Validate header only:
  ```bash
  ampctl dataset verify --mode header
  ```
- Validate transactions only:
  ```bash
  ampctl dataset verify --mode transactions
  ```
- Validate receipts only:
  ```bash
  ampctl dataset verify --mode receipts
  ```
- Validate header accumulators with external data:
  ```bash
  ampctl dataset verify --mode header-accumulators --data-path "/data/header-accumulators"
  ```

## Exit Codes

- `0`: Verification succeeded
- non-zero: Verification failed or required external data missing/invalid
