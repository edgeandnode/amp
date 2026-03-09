# Dataset Rematerialize Feature - Implementation Plan

## Overview

Implement an `ampctl dataset rematerialize` command that allows re-extracting a given block range for a raw dataset. The feature enables users to refresh historical data by re-extracting specific block ranges without manual intervention.

## Goal

Provide a command-line interface for rematerializing (re-extracting) block ranges in raw datasets. The feature should:

1. Accept a dataset reference and block range (start/end blocks)
2. Send a rematerialize request to an active materialization job if one exists
3. Create a temporary job if no active job exists
4. Have the active job process the rematerialize range before resuming normal operation
5. Rely on segment resolution (newer files automatically become canonical via `last_modified` timestamps)

## Design Decisions

### Scope

- **Raw datasets only** - Derived datasets return an error with appropriate message
- **No automatic propagation** - Users must manually re-deploy derived datasets after rematerializing raw datasets

### Request Delivery

- Use existing `job_actions` PostgreSQL NOTIFY channel
- Add new `Rematerialize { start_block, end_block }` action variant
- Custom serde for backward compatibility (strings for Start/Stop, object for Rematerialize)

### Data Handling

- **Don't delete existing files** - Rely on segment resolution to prefer newer files based on timestamps
- **Segment alignment** - Extend requested range to match canonical chain segment boundaries
- **Clean re-extraction** - Avoids partial overlaps by processing complete segments

### Execution Mode

- **Block normal dump loop** - Process rematerialize ranges first, then resume normal operation
- **Request queueing** - Queue all rematerialize requests (don't merge or reject)
- **Non-blocking checks** - Use `try_recv()` to check for requests without blocking the dump loop

### Validation

- **Block range validation** - Ensure `start_block <= end_block` and `start_block >= dataset.start_block()`
- **Dataset type check** - Reject requests for derived datasets

### Temporary Jobs

- **Start when no active job exists** - Allow rematerialization even when dataset isn't actively running
- **Modified job descriptor** - Add `start_block_override` and `rematerialize_only` fields
- **Single-pass execution** - Exit after processing rematerialize range (don't continue to normal operation)

## Architecture

### Request Flow

```
ampctl CLI
    ↓
Admin API (/datasets/{namespace}/{name}/versions/{revision}/rematerialize)
    ↓
PostgreSQL NOTIFY (job_actions channel)
    ↓
Worker Service (notification handler)
    ↓
JobSet (mpsc::Sender)
    ↓
Dump Loop (mpsc::Receiver)
    ↓
Re-extract aligned ranges
```

### Component Responsibilities

#### CLI (`ampctl dataset rematerialize`)

- Parse command-line arguments (dataset reference, start-block, end-block)
- Call Admin API rematerialize endpoint
- Display success/error status to user

#### Admin API Handler

- Validate dataset is raw (not derived)
- Validate block ranges (start <= end, start >= dataset.start_block())
- Find active jobs for the dataset
- Send PostgreSQL NOTIFY with Rematerialize action
- Return job_id and new_job_created flag

#### Worker Service

- Listen for job notifications on `job_actions` channel
- Forward Rematerialize actions to running jobs via mpsc channel
- Track rematerialize senders in JobSet

#### Dump Loop

- Check for rematerialize requests (non-blocking `try_recv()`)
- For each request:
  - Get segments for the table
  - Align range to segment boundaries
  - Add aligned range to missing_ranges
- Process rematerialize ranges before normal missing ranges

#### Segment Alignment Helper

- Expand requested range to include full overlapping segments
- Ensures clean re-extraction without partial overlaps
- Returns aligned range (may be larger than requested)

## Progress Summary

### ✅ Completed (7/10 tasks)

#### 1. Action Enum Extension

- **File:** `crates/services/worker/src/job/notif.rs`
- **Changes:**
  - Added `Rematerialize { start_block, end_block }` variant
  - Implemented custom serde for backward compatibility
  - Added comprehensive tests

#### 2. Admin API Endpoint

- **File:** `crates/services/admin-api/src/handlers/datasets/rematerialize.rs`
- **Changes:**
  - Implemented `POST /datasets/{namespace}/{name}/versions/{revision}/rematerialize`
  - Validates dataset is raw (not derived)
  - Validates block ranges against dataset bounds
  - Finds active jobs and sends notifications
  - Returns `RematerializeResponse` with job_id and new_job_created flag
  - Returns error `NO_ACTIVE_JOB` when no active job exists (temporary job creation pending)

#### 3. MetadataDb Helper

- **File:** `crates/core/metadata-db/src/lib.rs`
- **Changes:**
  - Added `send_job_notif()` convenience method
  - Wraps `metadata_db::workers::send_job_notif()`

#### 4. Admin Client Method

- **File:** `crates/clients/admin/src/datasets.rs`
- **Changes:**
  - Added `rematerialize()` method to `DatasetsClient`
  - Handles all error cases from the API
  - Returns `RematerializeResponse`

#### 5. CLI Command

- **Files:**
  - `crates/bin/ampctl/src/cmd/dataset/rematerialize.rs` (new)
  - `crates/bin/ampctl/src/cmd/dataset.rs` (modified)
- **Changes:**
  - Created `ampctl dataset rematerialize <REFERENCE> --start-block <N> --end-block <N>`
  - Alias: `ampctl dataset remat`
  - Pretty console output with colors

#### 6. Worker Service Handling

- **Files:**
  - `crates/services/worker/src/service/job_set.rs`
  - `crates/services/worker/src/service.rs`
  - `crates/services/worker/src/service/error.rs`
- **Changes:**
  - Extended `JobSet` with channel management for rematerialize requests
  - Added `create_job_channel()` and `spawn_with_existing_channel()` methods
  - Added `send_rematerialize()` method to send requests to running jobs
  - Updated `handle_job_notification_action()` to handle Rematerialize action
  - Added `NotificationError::RematerializeActionFailed` variant

#### 7. Dump Loop Integration

- **Files:**
  - `crates/core/worker-core/src/lib.rs` - Added shared `RematerializeRequest` type
  - `crates/services/worker/src/service/job_impl.rs` - Pass receiver to execute()
  - `crates/core/worker-datasets-raw/src/job_impl.rs` - Integrate in materialize loop (was dataset.rs before refactor)
  - `crates/core/worker-datasets-raw/src/job_impl/ranges.rs` - Helper functions (conflict resolved during rebase)
  - `crates/core/common/src/physical_table/segments.rs` - Add alignment helper
  - `crates/core/common/src/physical_table/table.rs` - Make segments() public
- **Changes:**
  - Passed rematerialize_rx through job spawning chain
  - Modified `execute()` signature to accept rematerialize receiver (function renamed from `dump()` in PR #1943)
  - Added non-blocking check for rematerialize requests in materialize loop
  - Implemented segment boundary alignment via `align_to_segment_boundaries()`
  - Added aligned ranges to missing_ranges for re-extraction
  - Added `Error::GetSegments` variant with proper error handling
- **Note:** Successfully adapted to codebase refactor (PR #1943) which split `dataset.rs` into `job_impl.rs` and `job_impl/ranges.rs`

### 📋 Remaining Work (2 tasks, 1 deferred)

#### 8. Segment Boundary Alignment Tests

- **Status:** TODO
- **Files to create/modify:**
  - `crates/core/common/src/physical_table/segments.rs` (add tests)
- **Work needed:**
  - Unit tests for `align_to_segment_boundaries()` function
  - Test cases:
    - Range with no overlapping segments (returns original range)
    - Range overlapping single segment (expands to segment boundaries)
    - Range overlapping multiple segments (expands to cover all)
    - Range fully contained in segment (expands to segment boundaries)
    - Range spanning gap between segments (expands to both segment boundaries)

#### 9. Temporary Job Creation

- **Status:** DEFERRED (to be implemented in future PR)
- **Reason:** Adds complexity to job scheduling system; initial implementation focuses on active job scenario
- **Current Behavior:** API returns `NO_ACTIVE_JOB` error when no active job exists for the dataset
- **User Impact:** Users must ensure dataset has an active job (e.g., via deployment) before rematerializing
- **Future Work:**
  - Add optional `start_block_override: Option<u64>` to `JobDescriptor::MaterializeRaw`
  - Add optional `rematerialize_only: bool` to `JobDescriptor::MaterializeRaw`
  - When `NO_ACTIVE_JOB` error occurs in admin API:
    - Create new job with modified descriptor
    - Set `start_block_override` to rematerialize start_block
    - Set `rematerialize_only` to true
    - Schedule job via controller
  - Modify materialize loop to:
    - Start from `start_block_override` if present (instead of dataset.start_block())
    - Exit after one pass if `rematerialize_only` is true
    - Don't wait for more data in rematerialize_only mode

#### 10. Integration Tests

- **Status:** TODO (reduced scope - no temporary job tests)
- **Files to create:**
  - `crates/services/admin-api/tests/rematerialize.rs` (new) - API endpoint tests
  - `crates/core/worker-datasets-raw/tests/rematerialize.rs` (new) - Worker integration tests
- **Test scenarios:**
  - **Basic flow (happy path):**
    - Deploy dataset with active job
    - Send rematerialize request
    - Verify job receives request
    - Verify ranges are aligned to segments
    - Verify new files are created
    - Verify new files become canonical (newer timestamps)
  - **Error cases:**
    - No active job (should return `NO_ACTIVE_JOB` error)
    - Derived dataset (should return error)
    - Invalid block ranges (start > end)
    - Block range before dataset.start_block()
    - Non-existent dataset
  - **Edge cases:**
    - Multiple rematerialize requests queued
    - Rematerialize request for range with no existing segments
    - Rematerialize request for range already being extracted

## Key Files Modified

### Core Type Definitions

```
crates/core/worker-core/src/lib.rs                   # RematerializeRequest type
crates/services/worker/src/job/notif.rs              # Action enum
```

### API Layer

```
crates/services/admin-api/src/handlers/datasets/rematerialize.rs  # Endpoint handler
crates/services/admin-api/src/handlers/datasets.rs   # Module registration
crates/services/admin-api/src/lib.rs                 # Route registration
crates/clients/admin/src/datasets.rs                 # Client method
```

### Worker Service

```
crates/services/worker/src/service/job_set.rs        # Channel management
crates/services/worker/src/service/job_impl.rs       # Pass receiver to dump
crates/services/worker/src/service.rs                # Notification handling
crates/services/worker/src/service/error.rs          # Error variants
```

### Dump Implementation

```
crates/core/worker-datasets-raw/src/dataset.rs       # Dump loop integration
crates/core/common/src/physical_table/segments.rs    # Segment alignment
crates/core/common/src/physical_table/table.rs       # Public segments() method
```

### CLI

```
crates/bin/ampctl/src/cmd/dataset/rematerialize.rs   # Command implementation
crates/bin/ampctl/src/cmd/dataset.rs                 # Command registration
```

### Database Layer

```
crates/core/metadata-db/src/lib.rs                   # Helper method
```

## Technical Details

### Segment Resolution

The `canonical_chain()` function in `segments.rs` automatically prefers segments with newer `last_modified` timestamps when multiple segments cover the same range. This means:

- No need to delete existing files
- New extractions automatically become canonical
- Old files remain in storage but are not queried
- Garbage collection will eventually remove old files

### Segment Alignment Algorithm

```rust
pub fn align_to_segment_boundaries(
    segments: &[Segment],
    requested: RangeInclusive<BlockNum>,
) -> RangeInclusive<BlockNum>
```

For each segment that overlaps with the requested range:

1. Check if segment intersects with requested range
2. If yes, expand aligned range to include entire segment
3. Return expanded range that covers all overlapping segments

Example:

```
Requested:  [100..150]
Segments:   [0..99], [100..199], [200..299]
Aligned:    [100..199]  (expanded to cover segment 100..199)
```

### Message Flow for Active Job

```
1. User: ampctl dataset rematerialize mainnet/blocks --start-block 1000 --end-block 2000
2. Admin API: Validate, find active job, send NOTIFY
3. Worker: Receive notification, forward to JobSet
4. JobSet: Send RematerializeRequest via mpsc channel
5. Dump loop: Receive request, align to segments, add to missing_ranges
6. Dump loop: Extract aligned range, write new files
7. Segment resolution: New files become canonical (newer timestamps)
```

### Message Flow for Temporary Job

```
1. User: ampctl dataset rematerialize mainnet/blocks --start-block 1000 --end-block 2000
2. Admin API: Validate, find NO active job, create temporary job
3. Admin API: Set start_block_override=1000, rematerialize_only=true
4. Controller: Schedule new job
5. Worker: Start job, create rematerialize channel
6. Dump loop: Start from start_block_override, process range, exit
7. Segment resolution: New files become canonical
```

## Next Steps

### Immediate (Current PR)

1. **Add CLI/API documentation for "active job required" limitation** ✅ NEXT
   - Update CLI help text to mention active job requirement
   - Add API documentation note about `NO_ACTIVE_JOB` error
   - Document workaround (deploy dataset first)

2. **Implement segment alignment tests** (Task 8)
   - Write unit tests for `align_to_segment_boundaries()`
   - Verify correct behavior for all edge cases

3. **Write integration tests** (Task 10)
   - Test complete flow with active jobs
   - Test error cases (including NO_ACTIVE_JOB)
   - Test edge cases

4. **Final validation**
   - Run full test suite
   - Verify all clippy warnings resolved
   - Code review readiness check

### Future Work (Separate PR)

1. **Implement temporary job creation** (Task 9 - deferred)
   - Design job descriptor extensions
   - Implement admin API job scheduling
   - Update materialize loop to handle temporary jobs
   - Add tests for temporary job scenario

2. **Enhanced features**
   - Automatic derived dataset re-deployment
   - Rematerialize progress tracking
   - Batch rematerialization for multiple ranges

### Post-Deployment

1. **Documentation**
   - Update user-facing documentation
   - Add examples and tutorials
   - Document derived dataset propagation requirements

2. **Monitoring**
   - Test in staging environment
   - Verify backwards compatibility
   - Monitor performance impact
   - Collect user feedback on "active job required" limitation
