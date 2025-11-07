# AmpRegistry Publish Flow - Architecture Plan

## Overview

Design and implement the `AmpRegistryService` to enable publishing datasets and versions to the Amp Registry API, along with refactoring the `publish` CLI command to use this service.

---

## Phase 1: AmpRegistryService - Core Design

### 1.1 Service Structure ✅

- [x] Service name: `"Amp/Services/AmpRegistryService"`
- [x] Dependencies: `FetchHttpClient.layer`, `Auth.layer`
- [x] Base URL: Configurable (default: `http://localhost:4000`, production: `https://registry.amp.staging.edgeandnode.com`)

### 1.2 Schema Definitions (Request DTOs) ✅

- [x] Export `AmpRegistryInsertDatasetDto` (already defined, needs export)
- [x] Export `AmpRegistryInsertDatasetVersionDto` (already defined, needs export)

### 1.3 Schema Definitions (Response DTOs) ✅

These schemas match the OpenAPI response models:

- [x] `AmpRegistryDatasetVersionAncestryDto` - Supporting schema for version ancestry relationships
- [x] `AmpRegistryDatasetVersionDto` - Full dataset version response
- [x] `AmpRegistryDatasetDto` - Full dataset response from API
- [x] `AmpRegistryErrorResponseDto` - Standard error format with `error_code`, `error_message`, `request_id`

**Implementation Details:**
- All Response DTOs use snake_case field names (consistent with API)
- Reuse Model.ts schemas wherever possible (DatasetNamespace, DatasetName, DatasetRevision, etc.)
- Use `Schema.optionalWith({ nullable: true })` for optional/nullable fields
- Timestamps kept as strings (no DateTimeUtc encoding/decoding)
- Owner field uses `Schema.Union(Schema.NonEmptyTrimmedString, Model.Address)` for validation
- `Model.Address` exported from Model.ts (uses viem's `isAddress()` validator)

### 1.4 Error Types ✅

Define custom errors for the service:

- [x] `DatasetAlreadyExistsError` - Dataset with namespace/name already exists (fields: `namespace`, `name`)
- [x] `DatasetNotFoundError` - Dataset doesn't exist (fields: `namespace`, `name`)
- [x] `VersionAlreadyExistsError` - Version tag already exists for dataset (fields: `namespace`, `name`, `versionTag`)
- [x] `DatasetOwnershipError` - User attempting to publish to a dataset they don't own (fields: `namespace`, `name`, `actualOwner`, `userAddresses`)
- [x] `RegistryApiError` - Generic API error (fields: `status`, `errorCode`, `message`, `requestId?`)

**Implementation Details:**
- All errors use `Data.TaggedError` pattern (consistent with Auth.ts)
- Error names follow `Amp/Registry/Errors/` namespace convention
- All fields are `readonly` for immutability
- `RegistryApiError.requestId` is optional (for tracing)

---

## Phase 2: AmpRegistryService - Methods ✅ COMPLETED

### 2.1 Dataset Existence Check ✅

**Method:** `getDataset(namespace: Model.DatasetNamespace, name: Model.DatasetName)`

- **Purpose:** Get dataset details (if exists) to determine ownership and next action
- **API Endpoint:** `GET /api/v1/datasets/{namespace}/{name}` (public endpoint)
- **Authentication:** Not required
- **Returns:** `Effect<Option<AmpRegistryDatasetDto>, RegistryApiError>`
- **Implementation:** AmpRegistry.ts:54-68
- **Logic:**
  - 200 OK → dataset exists → return `Option.some(dataset)`
  - 404 Not Found → dataset doesn't exist → return `Option.none()`
  - Other errors → parse error response and return `RegistryApiError`

### 2.2 Publish New Dataset ✅

**Method:** `publishDataset(auth: Auth.AuthStorageSchema, dto: AmpRegistryInsertDatasetDto)`

- **Purpose:** Publish a brand new dataset (first time)
- **API Endpoint:** `POST /api/v1/owners/@me/datasets/publish`
- **Authentication:** Required (Bearer token from auth.accessToken)
- **Request Body:** `InsertDatasetDto` (JSON, encoded via Schema.encode)
- **Returns:** `Effect<AmpRegistryDatasetDto, RegistryApiError>`
- **Implementation:** AmpRegistry.ts:76-96
- **Logic:**
  - Encode DTO to JSON body
  - Build HTTP POST request with Bearer token
  - Expect 201 Created status
  - Parse response as `AmpRegistryDatasetDto`
  - Handle errors via `toRegistryError` helper

### 2.3 Publish New Version ✅

**Method:** `publishVersion(auth: Auth.AuthStorageSchema, namespace: Model.DatasetNamespace, name: Model.DatasetName, dto: AmpRegistryInsertDatasetVersionDto)`

- **Purpose:** Publish a new version to an existing dataset
- **API Endpoint:** `POST /api/v1/owners/@me/datasets/{namespace}/{name}/versions/publish`
- **Authentication:** Required (Bearer token from auth.accessToken)
- **Request Body:** `InsertDatasetVersionDto` (JSON, encoded via Schema.encode)
- **Returns:** `Effect<AmpRegistryDatasetVersionDto, RegistryApiError>`
- **Implementation:** AmpRegistry.ts:106-133
- **Logic:**
  - Encode DTO to JSON body
  - Build HTTP POST request with Bearer token
  - Expect 201 Created status
  - Parse response as `AmpRegistryDatasetVersionDto`
  - Handle errors via `toRegistryError` helper

### 2.4 High-Level Publish Flow ✅

**Method:** `publishFlow(args: { auth, context, versionTag, changelog? })`

- **Purpose:** High-level orchestration method to publish a dataset or version
- **Implementation:** AmpRegistry.ts:145-248
- **Returns:** `Effect<Model.DatasetReference, DatasetOwnershipError | VersionAlreadyExistsError | RegistryApiError>`
- **Logic:**
  - Derives `indexingChains` from manifest tables (line 179)
  - Calls `getDataset` to check if dataset exists
  - **If dataset exists:**
    - Validates ownership (compares dataset.owner with auth.accounts)
    - Checks if version exists using `Array.findFirst` on dataset.versions
    - Publishes new version if doesn't exist
  - **If dataset doesn't exist:**
    - Publishes new dataset with initial version
  - Returns `DatasetReference` with namespace/name@versionTag

**Key Improvements:**
- Single `args` object parameter (more maintainable)
- Derives `indexingChains` from manifest.tables instead of requiring parameter
- Converts `dependencies` to `DatasetReferenceStr` for ancestors field
- Uses `Array.findFirst` for idiomatic Option-based version checking
- Respects `status` and `visibility` from metadata with sensible defaults

---

## Phase 3: CLI Command Refactor - `publish.ts`

### 3.1 Command Arguments

- [x] `tag` - Version tag (semver, commit hash, or "dev")
- [x] `configFile` - Path to dataset config file (optional, will search)

### 3.2 Command Flow

```
1. Parse CLI args (tag, configFile)
2. Build ManifestContext from config file
3. Extract dataset metadata & manifest
4. Get AmpRegistryService instance
5. Check if dataset exists:
   a. If NO → Publish new dataset (includes first version)
   b. If YES → Validate ownership (check if dataset.owner is in auth.accounts)
      - If not owner → Error (DatasetOwnershipError)
      - If owner → Check if version exists:
         * If version exists → Error (VersionAlreadyExistsError)
         * If version doesn't exist → Publish new version
6. Display success message with dataset reference
```

### 3.3 Refactoring Tasks

- [ ] Remove duplicate DTOs (lines 21-45) - use exports from `AmpRegistry.ts`
- [ ] Remove direct HTTP client usage (lines 78-110)
- [ ] Replace with `AmpRegistryService` method calls
- [ ] Add proper error handling for all error types
- [ ] Add user-friendly console messages for each step

### 3.4 Dependencies

- [ ] Add `AmpRegistry.AmpRegistryService` to command dependencies
- [x] Keep existing: `Auth.layer`, `FetchHttpClient.layer`, `ManifestContext.layer`

---

## Phase 4: Auth Service Enhancement ✅ COMPLETED

### 4.1 Problem Statement (RESOLVED)

~~**Blocker:** Ownership validation requires comparing user's 0x Ethereum address with dataset `owner` field, but Auth service only exposes `userId` (Privy user ID format).~~

**✅ RESOLVED:** Auth service now provides user's linked accounts including Ethereum addresses.

### 4.2 Implemented Solution

**Option C was implemented:** RefreshTokenResponse includes user's accounts array

The Auth service now exposes an `accounts` array containing the user's linked accounts (including 0x Ethereum addresses):

```typescript
// RefreshTokenResponse (Auth.ts:39-45)
user: Schema.Struct({
  id: AuthUserId,
  accounts: Schema.Array(Schema.Union(Schema.NonEmptyTrimmedString, Address))
})

// AuthStorageSchema (Auth.ts:51-57)
export class AuthStorageSchema extends Schema.Class<AuthStorageSchema>(...) ({
  accessToken: Schema.NonEmptyTrimmedString,
  refreshToken: Schema.NonEmptyTrimmedString,
  userId: AuthUserId,
  accounts: Schema.Array(Schema.Union(Schema.NonEmptyTrimmedString, Address)).pipe(Schema.optional),
  expiry: Schema.Int.pipe(Schema.positive(), Schema.optional),
})
```

### 4.3 Acceptance Criteria ✅

- [x] Auth service exposes user's Ethereum 0x address(es)
- [x] Addresses are available from `auth.get()` and `auth.getRequired()` methods via `accounts` field
- [x] Addresses are validated using viem's `isAddress()` function
- [x] Addresses are persisted in KeyValueStore alongside tokens
- [x] Token refresh automatically populates missing `accounts` field (Auth.ts:196)

---

## Phase 5: Decision Points & Questions

### 5.1 Dataset Existence Strategy ✅

**Decision:** Use public endpoint `GET /api/v1/datasets/{namespace}/{name}` and validate ownership manually

- ✅ Uses public endpoint (no auth required for read)
- ✅ Single source of truth (get full dataset info)
- ✅ Catches ownership violations early with clear error messages
- ✅ Avoids ambiguous 403 errors from write endpoints

### 5.2 Version Existence Strategy ✅

**Decision:** Use public endpoint `GET /api/v1/datasets/{namespace}/{name}/versions/{version}` to check existence

- ✅ Clear error message: "Version 1.0.0 already exists"
- ✅ One extra API call, but better UX

### 5.3 Base URL Configuration

**Question:** Should base URL be configurable?

**Current:** Hardcoded to `http://localhost:4000` (AmpRegistry.ts:10)
**Needed:** Support staging (`https://registry.amp.staging.edgeandnode.com`) and production

**Options:**

- Environment variable (e.g., `AMP_REGISTRY_URL`)
- CLI flag (e.g., `--registry-url`)
- Config file setting
- Combination (env var with CLI override)

**Decision:** TBD - needs user input

---

## Phase 6: Implementation Checklist

### 6.1 Auth.ts ✅ COMPLETED

- [x] Add Ethereum address to `AuthStorageSchema` (via `accounts` array)
- [x] Update `RefreshTokenResponse` schema (includes `user.accounts`)
- [x] Expose Ethereum addresses from `get()` method (via `accounts` field)
- [x] Expose Ethereum addresses from `getRequired()` method (via `accounts` field)
- [x] Add validation for 0x address format (using viem's `isAddress()`)
- [ ] Update tests (if needed)

### 6.2 AmpRegistry.ts ✅ COMPLETED

- [x] Add response schema definitions (Dataset, DatasetVersion, ErrorResponse)
- [x] Add error types (DatasetOwnershipError, VersionAlreadyExistsError, etc.)
- [x] Implement `getDataset` method
- [x] Implement `publishDataset` method
- [x] Implement `publishVersion` method
- [x] Implement `publishFlow` orchestration method
- [x] Export DTOs for external use
- [x] Add JSDoc documentation for all methods
- [x] Fix error handling - `parseRegistryError` helper with proper Effect.flip usage
- [ ] Handle base URL configuration (currently hardcoded to localhost:4000)

**Bug Fixes:**
- Fixed `RegistryApiError` being wrapped in `Effect` in error union (was causing TypeScript errors)
- Renamed `toRegistryError` → `parseRegistryError` to clarify it returns an Effect
- Updated `getDataset`: Use `Effect.catchAll` + `Effect.flip` for proper error handling
- Updated `publishDataset` and `publishVersion`: Use `Effect.flip` to convert success Effect to error Effect

### 6.3 AmpRegistry.test.ts ✅ COMPLETED

Comprehensive test suite created and validated for AmpRegistryService.

**Test Coverage:** 15 tests, all passing ✅

**Test Structure:**
- [x] Set up test fixtures (mock auth data, datasets, versions, manifest context)
- [x] Mock HTTP client responses for each endpoint
- [x] Test layer setup with dependencies

**Public Method Tests:**
- [x] Test `getDataset()` - success (returns Option.some with dataset)
- [x] Test `getDataset()` - not found (returns Option.none)
- [x] Test `getDataset()` - API error (returns RegistryApiError)

**Authenticated Method Tests:**
- [x] Test `publishDataset()` - success (returns created dataset)
- [x] Test `publishDataset()` - API error 400/500 (returns RegistryApiError)
- [x] Test `publishVersion()` - success (returns created version)
- [x] Test `publishVersion()` - API error 400/403/500 (returns RegistryApiError)

**High-Level Flow Tests (publishFlow):**
- [x] Test new dataset publish - success (dataset doesn't exist → publishDataset)
- [x] Test new version publish - success (dataset exists, user owns, version doesn't exist → publishVersion)
- [x] Test ownership error (dataset exists, user doesn't own → DatasetOwnershipError)
- [x] Test version exists error (dataset exists, version exists → VersionAlreadyExistsError)
- [x] Test indexingChains derivation from manifest.tables
- [x] Test ancestors conversion to DatasetReferenceStr
- [x] Test status/visibility defaults

**Error Response Parsing:**
- [x] Test parsing AmpRegistryErrorResponseDto from failed responses
- [x] Test handling responses without error_code/error_message
- [x] Test request_id propagation in RegistryApiError (simplified assertions)

**Implementation Details:**
- Uses `@effect/vitest` with `it.effect` API for idiomatic Effect testing
- Proper Schema.Class construction with `.make()` methods
- Mock HTTP client using `HttpClient.make()` with proper `HttpClientError.RequestError` types
- Layer management with `AmpRegistryService.DefaultWithoutDependencies`
- All handlers use `Effect.gen` pattern for consistent error handling
- File: `test/AmpRegistry.test.ts`

### 6.4 publish.ts ✅ COMPLETED

Publish command fully implemented using AmpRegistryService.

**Implementation Summary:**
- ✅ Clean imports - using `AmpRegistry` namespace
- ✅ Command args properly defined: `tag` (optional), `changelog` (optional), `configFile`, `adminUrl`
- ✅ Service dependencies wired: `Auth.layer`, `AmpRegistry.layer`, `ManifestContext.layer`
- ✅ Services injected: `ManifestContext`, `Auth.AuthService`, `AmpRegistry.AmpRegistryService`
- ✅ Auth check implemented with friendly error message
- ✅ Version tag handling (defaults to "dev" if not provided)
- ✅ Status determination ("draft" for "dev", "published" otherwise)
- ✅ `publishFlow()` call with all required arguments
- ✅ Comprehensive error handling with `Effect.catchTags`:
  - ✅ `DatasetOwnershipError` - Shows namespace/name and ownership info
  - ✅ `VersionAlreadyExistsError` - Shows version conflict message
  - ✅ `RegistryApiError` - Shows API error details (status, code, message, requestId)
- ✅ Success message with published dataset reference
- ✅ Uses `Effect.tap` for side effects (success logging)
- ✅ No emojis (following project conventions)

**Key Implementation Details:**
```typescript
// Version tag handling
const versionTag = Option.getOrElse(args.tag, () => "dev" as const)
const status = versionTag === "dev" ? "draft" : "published"

// Publish with error handling
yield* ampRegistry.publishFlow({
  auth: accessToken,
  context,
  versionTag,
  changelog: Option.getOrUndefined(args.changelog),
  status,
}).pipe(
  Effect.tap((result) => Console.log(`Published ${result.namespace}/${result.name}@${result.revision}`)),
  Effect.catchTags({
    "Amp/Registry/Errors/DatasetOwnershipError": (err) => /* console errors */,
    "Amp/Registry/Errors/VersionAlreadyExistsError": (err) => /* console errors */,
    "Amp/Registry/Errors/RegistryApiError": (err) => /* console errors */,
  }),
)
```

**Bug Fixes Applied:**
- Fixed `RegistryApiError` being wrapped in `Effect` in error union
- Renamed `toRegistryError` → `parseRegistryError` for clarity
- Updated error handling to use `Effect.flip` to properly convert success Effect to error Effect
- All type errors resolved

### 6.5 AmpRegistry.test.ts (Original Checklist - Reference Only)

This section preserved for historical reference. See section 6.3 for completed implementation.

<details>
<summary>Original test checklist (now completed)</summary>

**Test Structure:**
- [x] Set up test fixtures (mock auth data, datasets, versions)
- [x] Mock HTTP client responses for each endpoint
- [x] Test layer setup with dependencies

**Public Method Tests:**
- [x] Test `getDataset()` - success, not found, API error

**Authenticated Method Tests:**
- [x] Test `publishDataset()` - success, API errors
- [x] Test `publishVersion()` - success, API errors

**Error Response Parsing:**
- [x] Test parsing AmpRegistryErrorResponseDto from failed responses
- [x] Test handling responses without error_code/error_message
- [x] Test request_id propagation in RegistryApiError

**Schema Validation:**
- [x] Test Response DTO decoding (valid API responses)
- [x] Test Response DTO decoding errors (malformed responses)

</details>

### 6.6 End-to-End Testing (publish command) - **NEXT PRIORITY** 🎯

Manual testing with real config files to validate the complete flow.

**Test Scenarios:**
- [ ] Test publishing new dataset (first time)
- [ ] Test publishing new version to existing dataset
- [ ] Test error: dataset owned by different user
- [ ] Test error: version already exists
- [ ] Test error: authentication failure (no auth token)
- [ ] Test error: invalid manifest/metadata
- [ ] Test with "dev" tag (should create draft status)
- [ ] Test with semver tag (should create published status)
- [ ] Test with changelog option

**Prerequisites:**
- Running Amp Registry instance (localhost:4000 or staging)
- Valid dataset config file
- Authenticated user (run `amp auth login`)

---

## Phase 7: Future Enhancements (Out of Scope)

These are not needed for the initial implementation but may be useful later:

- [ ] `listOwnedDatasets()` - List user's datasets
- [ ] `searchDatasets(query)` - Search public datasets
- [ ] `getManifest(namespace, name, version)` - Fetch manifest JSON
- [ ] `updateDatasetMetadata()` - Update dataset description, keywords, etc.
- [ ] `updateVersionStatus()` - Change version status (draft ↔ published)
- [ ] `archiveVersion()` - Archive a version
- [ ] Stream-based pagination helpers for list operations
- [ ] SSE stream support for real-time dataset updates

---

## Notes & Considerations

### Error Handling Philosophy

- Auth errors (`AuthTokenNotFoundError`, etc.) should propagate directly - they indicate user needs to login
- Registry API errors should be wrapped in descriptive custom errors when possible
- Always include context in error messages (dataset name, version, operation)

### Manifest & Metadata Mapping

The `ManifestContext.DatasetContext` contains:

- `metadata` - User-provided metadata (name, namespace, description, etc.)
- `manifest` - Built manifest JSON (the actual dataset definition)
- `dependencies` - Array of DatasetReference (for `ancestors` field)

We map this to the DTOs:

- `metadata` → DTO metadata fields (name, namespace, description, keywords, etc.)
- `manifest` → DTO `version.manifest` field
- `dependencies` → DTO `version.ancestors` field

### Version Tag Handling

- User provides: semver (e.g., "1.0.0"), commit hash (e.g., "8e0acc0"), or "dev"
- If "dev" or not provided → status: "draft"
- If specific version → status: "published"
- Registry API validates version tag format server-side

### Ownership Validation ✅

- Dataset `owner` field is Ethereum 0x address
- Auth service exposes user's accounts array (includes 0x addresses)
- Validate ownership by checking if `dataset.owner` is in `auth.accounts` (filtered for addresses using `isAddress()`)

---

## Success Criteria

The implementation is complete when:

1. ✅ User can publish a new dataset with `amp publish --tag 1.0.0`
2. ✅ User can publish a new version with `amp publish --tag 1.1.0` (to existing dataset)
3. ✅ Clear error if dataset is owned by another user
4. ✅ Clear error if version already exists
5. ✅ Clear error if user is not authenticated
6. ✅ No duplicate code between AmpRegistry.ts and publish.ts
7. ✅ Service methods are well-documented and reusable
8. ✅ Auth service exposes user's Ethereum address for ownership validation

---

## Implementation Order

1. ~~**FIRST:** Fix Auth service to expose Ethereum address (Phase 4)~~ ✅ COMPLETED
2. ~~**NEXT:** Implement AmpRegistry service methods (Phase 6.2)~~ ✅ COMPLETED
3. ~~**THEN:** Build test suite for AmpRegistryService (Phase 6.3)~~ ✅ COMPLETED
4. ~~**THEN:** Implement publish command (Phase 6.4)~~ ✅ COMPLETED
5. **CURRENT:** Test end-to-end (Phase 6.6) 🎯
