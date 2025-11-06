# AmpRegistry Publish Flow - Architecture Plan

## Overview
Design and implement the `AmpRegistryService` to enable publishing datasets and versions to the Amp Registry API, along with refactoring the `publish` CLI command to use this service.

---

## Phase 1: AmpRegistryService - Core Design

### 1.1 Service Structure ✅
- [x] Service name: `"Amp/Services/AmpRegistryService"`
- [x] Dependencies: `FetchHttpClient.layer`, `Auth.layer`
- [x] Base URL: Configurable (default: `http://localhost:4000`, production: `https://registry.amp.staging.edgeandnode.com`)

### 1.2 Schema Definitions (Request DTOs)
- [ ] Export `AmpRegistryInsertDatasetDto` (already defined, needs export)
- [ ] Export `AmpRegistryInsertDatasetVersionDto` (already defined, needs export)

### 1.3 Schema Definitions (Response DTOs)
These schemas match the OpenAPI response models:
- [ ] `AmpRegistryDataset` - Full dataset response from API
- [ ] `AmpRegistryDatasetVersion` - Dataset version response
- [ ] `AmpRegistryErrorResponse` - Standard error format with `error_code`, `error_message`, `request_id`

### 1.4 Error Types
Define custom errors for the service:
- [ ] `DatasetAlreadyExistsError` - Dataset with namespace/name already exists
- [ ] `DatasetNotFoundError` - Dataset doesn't exist (for version publishing)
- [ ] `VersionAlreadyExistsError` - Version tag already exists for dataset
- [ ] `DatasetOwnershipError` - User attempting to publish to a dataset they don't own (with `namespace`, `name`, `actualOwner`, `attemptedBy` fields)
- [ ] `RegistryApiError` - Generic API error with status, error_code, message

---

## Phase 2: AmpRegistryService - Methods

### 2.1 Dataset Existence Check (Revised)
**Method:** `getDataset(namespace: string, name: string)`
- **Purpose:** Get dataset details (if exists) to determine ownership and next action
- **API Endpoint:** `GET /api/v1/datasets/{namespace}/{name}` (public endpoint)
- **Authentication:** Not required for the request, but we need authenticated user's 0x address to compare ownership
- **Returns:** `Effect<Option<AmpRegistryDataset>, RegistryApiError>`
- **Logic:**
  - 200 OK → dataset exists → return `Option.some(dataset)`
  - 404 Not Found → dataset doesn't exist → return `Option.none()`
  - Other errors → propagate as `RegistryApiError`

**⚠️ BLOCKER:** Ownership validation requires user's 0x address
- Dataset `owner` field is 0x address (e.g., `0x85F036b4952B74A438d724EA93495FD6220B94b6`)
- Auth service only provides `userId` (e.g., `c[a-z0-9]{24}` or `did:privy:c[a-z0-9]{24}`)
- **TODO:** Auth service needs to expose user's 0x address for ownership checks

**Usage in publish flow (once Auth provides 0x address):**
```typescript
const datasetOpt = yield* registry.getDataset(namespace, name)
const auth = yield* authService.getRequired()

if (Option.isSome(datasetOpt)) {
  const dataset = datasetOpt.value

  // Check ownership (REQUIRES: auth.ethereumAddress)
  if (dataset.owner !== auth.ethereumAddress) {
    yield* Effect.fail(new DatasetOwnershipError({
      namespace,
      name,
      actualOwner: dataset.owner,
      attemptedBy: auth.ethereumAddress
    }))
  }

  // User owns it → publish new version
  yield* registry.publishVersion(...)
} else {
  // Dataset doesn't exist → publish new dataset
  yield* registry.publishDataset(...)
}
```

### 2.2 Version Existence Check (Revised)
**Method:** `getVersion(namespace: string, name: string, versionTag: string)`
- **Purpose:** Get version details (if exists) to validate it doesn't already exist
- **API Endpoint:** `GET /api/v1/datasets/{namespace}/{name}/versions/{version}` (public endpoint)
- **Authentication:** Not required
- **Returns:** `Effect<Option<AmpRegistryDatasetVersion>, RegistryApiError>`
- **Logic:**
  - 200 OK → version exists → return `Option.some(version)`
  - 404 Not Found → version doesn't exist → return `Option.none()`
  - Other errors → propagate as `RegistryApiError`

**Usage in publish flow:**
```typescript
// First, get the dataset to check ownership
const datasetOpt = yield* registry.getDataset(namespace, name)
const auth = yield* authService.getRequired()

if (Option.isSome(datasetOpt)) {
  const dataset = datasetOpt.value

  // Check ownership (REQUIRES: auth.ethereumAddress)
  if (dataset.owner !== auth.ethereumAddress) {
    yield* Effect.fail(new DatasetOwnershipError(...))
  }

  // Check if version already exists
  const versionOpt = yield* registry.getVersion(namespace, name, versionTag)

  if (Option.isSome(versionOpt)) {
    // Version already exists → error
    yield* Effect.fail(new VersionAlreadyExistsError({
      namespace,
      name,
      versionTag
    }))
  }

  // Version doesn't exist → publish it
  yield* registry.publishVersion(...)
}
```

### 2.3 Publish New Dataset
**Method:** `publishDataset(dto: AmpRegistryInsertDatasetDto)`
- **Purpose:** Publish a brand new dataset (first time)
- **API Endpoint:** `POST /api/v1/owners/@me/datasets/publish`
- **Authentication:** Required (uses `auth.getRequired()`)
- **Request Body:** `InsertDatasetDto` (JSON)
- **Returns:** `Effect<AmpRegistryDataset, AuthErrors | RegistryApiError>`
- **Logic:**
  - Get auth token via `auth.getRequired()`
  - Build HTTP POST request with Bearer token
  - Parse response as `AmpRegistryDataset`
  - Handle errors (400, 401, 500, etc.)

### 2.4 Publish New Version
**Method:** `publishVersion(namespace: string, name: string, dto: AmpRegistryInsertDatasetVersionDto)`
- **Purpose:** Publish a new version to an existing dataset
- **API Endpoint:** `POST /api/v1/owners/@me/datasets/{namespace}/{name}/versions/publish`
- **Authentication:** Required (uses `auth.getRequired()`)
- **Request Body:** `InsertDatasetVersionDto` (JSON)
- **Returns:** `Effect<AmpRegistryDatasetVersion, AuthErrors | RegistryApiError>`
- **Logic:**
  - Get auth token via `auth.getRequired()`
  - Build HTTP POST request with Bearer token
  - Parse response as `AmpRegistryDatasetVersion`
  - Handle errors (400, 401, 403, 404, 500, etc.)

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
   b. If YES → Validate ownership (REQUIRES: auth.ethereumAddress)
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

## Phase 4: Auth Service Enhancement (PREREQUISITE)

### 4.1 Problem Statement
**Blocker:** Ownership validation requires comparing user's 0x Ethereum address with dataset `owner` field, but Auth service only exposes `userId` (Privy user ID format).

### 4.2 Required Changes to `Auth.ts`
The Auth service needs to expose the user's Ethereum address for ownership checks:

**Option A: Parse from auth token JWT claims**
- Decode the access token JWT (without verification, just reading claims)
- Extract Ethereum address from claims (if available)
- Add to `AuthStorageSchema` or return from `getRequired()`

**Option B: Fetch from auth platform API**
- Add endpoint call to get user profile/details
- Extract Ethereum address from response
- Cache alongside token

**Option C: Add to RefreshTokenResponse schema**
- Update `RefreshTokenResponse` to include user's Ethereum address
- Store in `AuthStorageSchema`
- Return from `get()` and `getRequired()`

**Recommendation:** Option C (if API supports it) or Option A (if address is in JWT claims)

### 4.3 Expected Auth Schema Update
```typescript
export class AuthStorageSchema extends Schema.Class<AuthStorageSchema>(...) ({
  accessToken: Schema.NonEmptyTrimmedString,
  refreshToken: Schema.NonEmptyTrimmedString,
  userId: AuthUserId,
  ethereumAddress: Schema.String.pipe(
    Schema.pattern(/^0x[a-fA-F0-9]{40}$/)
  ), // NEW: User's 0x address
})
```

### 4.4 Acceptance Criteria
- [ ] Auth service exposes user's Ethereum 0x address
- [ ] Address is available from `auth.get()` and `auth.getRequired()` methods
- [ ] Address is validated as proper 0x format
- [ ] Address is persisted in KeyValueStore alongside tokens

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

### 6.1 Auth.ts (PREREQUISITE)
- [ ] Add Ethereum address to `AuthStorageSchema`
- [ ] Update `RefreshTokenResponse` schema (if API provides it)
- [ ] Expose Ethereum address from `get()` method
- [ ] Expose Ethereum address from `getRequired()` method
- [ ] Add validation for 0x address format
- [ ] Update tests

### 6.2 AmpRegistry.ts
- [ ] Add response schema definitions (Dataset, DatasetVersion, ErrorResponse)
- [ ] Add error types (DatasetOwnershipError, VersionAlreadyExistsError, etc.)
- [ ] Implement `getDataset` method
- [ ] Implement `getVersion` method
- [ ] Implement `publishDataset` method
- [ ] Implement `publishVersion` method
- [ ] Export DTOs for external use
- [ ] Add JSDoc documentation for all methods
- [ ] Handle base URL configuration

### 6.3 publish.ts
- [ ] Import DTOs from AmpRegistry.ts
- [ ] Remove duplicate DTO definitions
- [ ] Inject AmpRegistryService dependency
- [ ] Implement dataset existence check logic
- [ ] Implement ownership validation
- [ ] Implement version existence check logic
- [ ] Call appropriate publish method (dataset vs version)
- [ ] Add error handling for all error types
- [ ] Add user-friendly console messages
- [ ] Test with real config files

### 6.4 Testing
- [ ] Test publishing new dataset (first time)
- [ ] Test publishing new version to existing dataset
- [ ] Test error: dataset owned by different user
- [ ] Test error: version already exists
- [ ] Test error: authentication failure
- [ ] Test error: invalid manifest/metadata

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

### Ownership Validation (CRITICAL)
- Dataset `owner` field is Ethereum 0x address
- Auth service must expose user's 0x address (not just `userId`)
- **BLOCKER:** Implementation cannot proceed without Auth service enhancement

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

1. **FIRST:** Fix Auth service to expose Ethereum address (Phase 4)
2. **SECOND:** Implement AmpRegistry service methods (Phase 6.2)
3. **THIRD:** Refactor publish command (Phase 6.3)
4. **FOURTH:** Test end-to-end (Phase 6.4)
