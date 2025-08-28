# Nozzle Registry API

A data layer and api for registry and discovery services for nozzle datasets within The Graph ecosystem. This platform serves as a centralized database and API layer to host, maintain, and make discoverable deployed nozzle datasets.

## Database Schema

The nozzle registry uses [GEL Database](https://docs.geldata.com/) with the following core models:

### Dataset Model

The `Dataset` type represents a deployed nozzle dataset and extends both `DatasetDiscovery` and `DatasetMetadata` abstract types.

**Core Properties:**

- `dataset_id` (required): Globally unique, content-addressable ID
- `name` (required): Slugified name (e.g., "ethereum-transfers")
- `namespace` (required): Grouping mechanism (org/team/wallet address)
- `status` (required): Current status (Draft, Published, Deprecated, Archived)
- `created_at` (required): Timestamp when dataset was created

**Discovery Properties (from DatasetDiscovery):**

- `description`: Dataset description (max 1024 chars)
- `keywords`: Array of usage keywords (e.g., "ERC20", "transfers", "NFT")
- `indexing_chains` (required): Array of CAIP-2 chain IDs (e.g., "eip155:1")
- `source` (required): Array of data sources (contracts, substreams, etc.)
- `visibility` (required): Private or Public (default: Public)

**Metadata Properties (from DatasetMetadata):**

- `owner` (required): ID of dataset owner
- `repository_url`: Optional GitHub repository URL
- `runtime_config`: Optional dynamic runtime configuration (JSON)
- `license`: Optional license information
- `mutability`: Dataset mutability (Mutable, Snapshot, AppendOnly)
- `ancestors`: Multi-link to other datasets this extends from

**Computed Properties:**

- `dataset_name`: Full name as "{namespace}/{name}"
- `latest_version`: Latest published version ordered by creation date
- `descendants`: Datasets that have this dataset as an ancestor

**Constraints:**

- `dataset_id` must be exclusive (unique)
- Combination of `namespace` and `name` must be exclusive
- `name` must match regex `^[a-z][a-z0-9_-]*$`

### DatasetVersion Model

The `DatasetVersion` type represents a deployed version of a Dataset. Each Dataset can have multiple versions.

**Properties:**

- `label` (required): Version label (e.g., "v0.0.1", "367e521")
- `dataset_version_id` (required): Content-addressable reference to this version
- `status` (required): Version status (default: Draft)
- `changelog`: Optional description of changes in this version
- `breaking`: Boolean indicating if version has breaking changes (default: false)
- `manifest` (required): Location of the dataset manifest
- `created_at` (required): Timestamp when version was created

**Computed Properties:**

- `dataset`: Backlink to the Dataset that owns this version

**Relationships:**

- Each DatasetVersion belongs to exactly one Dataset
- When a Dataset is deleted, orphaned DatasetVersions are also deleted

**Indexes:**

- Index on `dataset_version_id`
- Index on `label`

### Enums

**DatasetStatus:**

- `Draft`: Work in progress
- `Published`: Available for use
- `Deprecated`: No longer recommended
- `Archived`: Preserved but inactive

**DatasetVisibility:**

- `Private`: Only owner(s) can view/find
- `Public`: Anyone can discover

**DatasetMutability:**

- `Mutable`: Data can be modified
- `Snapshot`: Immutable point-in-time data
- `AppendOnly`: Data can only be added to

## Database Management Commands

Use these `just` commands for managing the GEL database migrations:

### Creating Migrations

```bash
# Create a new migration for schema changes
just nozzle_registry__migration_create
```

### Running Migrations

```bash
# Apply all unapplied migrations
# Note: Docker image runs all migrations on startup automatically
just nozzle_registry__migrate

# View log of applied migrations
just nozzle_registry__migrate_log
```

### Database Access

```bash
# Connect to the GEL CLI for direct database interaction
just nozzle_registry__gel_cli
```

## Full-Text Search and Indexing

The schema includes sophisticated indexing for dataset discovery:

- **Trigram Index**: Enables partial/fuzzy matching on dataset names
- **Full-Text Search**: Weight-ranked FTS index with priorities:
  1. **Weight A**: Keywords (highest priority)
  2. **Weight B**: Source information
  3. **Weight C**: Indexing chains
  4. **Weight D**: Description (lowest priority)

This allows for efficient searching across dataset metadata with relevance ranking based on the field where matches are found.

## Environment

Copy the [.env.example](./.env.example) to a `.env` file:

```bash
cp ./.env.example ./.env
```

### Database Configuration

- `GEL_SERVER_USER`: Database user (default: `nozzle_registry_adm`)
- `GEL_SERVER_PASSWORD`: Database password (default: `admin` for local dev)
- `GEL_SERVER_HOST`: Database host (default: `localhost`)
- `GEL_SERVER_PORT`: Database port (default: `5656`)
- `GEL_SERVER_CLIENT_TLS_SECURITY`: TLS mode (default: `insecure` for local dev)

### Authentication Configuration

- `AUTH_APP_ID`: Your Privy application ID
- `AUTH_APP_SECRET`: Your Privy app secret for server-side operations
- `JWKS_URL`: JWKS endpoint (e.g., `https://auth.privy.io/api/v1/apps/{app-id}/jwks.json`)
- `AUTH_USER_API_URL`: Privy user API endpoint (default: `https://api.privy.io/v1/users`)

### Optional Configuration

- `PRIVY_TEST_TOKEN`: Valid JWT token from Privy for testing authenticated endpoints
- `RUST_LOG`: Logging level control (e.g., `debug`, `info`, `warn`, `error`)

## Authentication

The API uses JWT authentication with Privy as the identity provider. Protected endpoints require a valid bearer token in
the Authorization header.

### Configuration

The authentication service validates JWTs against Privy's JWKS endpoint:

- JWKS URL: `https://auth.privy.io/api/v1/apps/{AUTH_APP_ID}/jwks.json`
- Algorithm: ES256 (Elliptic Curve)
- Token format: Bearer token in Authorization header

### How It Works

1. **Token Validation**: Incoming bearer tokens are validated against Privy's JWKS
2. **User Verification**: The user ID from the token is used to fetch the user from Privy's API
3. **Dual Validation**: Authentication succeeds only if BOTH the JWT is valid AND the user exists in Privy
4. **JWKS Caching**: Keys are cached for 1 hour to reduce network calls
5. **User Caching**: User data is also cached for 1 hour to minimize API calls
6. **Claims Extraction**: Valid tokens have their claims extracted (sub, sid, aud, iss, iat, exp)
7. **User ID**: The subject claim contains a DID format `did:privy:user_id`, which is parsed to extract the user ID

### Security Model

Authentication requires **both**:

- A valid JWT token properly signed by Privy
- The user must exist in the Privy IDaaS system

Even with a technically valid JWT, authentication will fail with `403 FORBIDDEN` if the user doesn't exist in Privy.
This provides defense-in-depth security and ensures deleted or suspended users are immediately blocked.

### Using Protected Routes

```rust
use axum::{Router, routing::get, Json};
use http_auth::{AuthenticatedUser, AuthService, auth_layer};
use serde_json::json;

// Protected endpoint handler
async fn protected_endpoint(user: AuthenticatedUser) -> Json<serde_json::Value> {
    // The AuthenticatedUser contains both JWT claims and user data from Privy
    Json(json!({
        "message": "Authenticated!",
        "user_id": user.user_id(), // Extracts user ID from did:privy:xxx format
        "full_subject": user.claims.sub,
        "session_id": user.claims.sid,
        "has_user_data": user.user.is_some(), // User data from Privy API
    }))
}

// Configure auth service with Privy credentials
let app_id = std::env::var("AUTH_APP_ID").expect("AUTH_APP_ID env var required");
let app_secret = std::env::var("AUTH_APP_SECRET").expect("AUTH_APP_SECRET env var required");
let jwks_url = format!("https://auth.privy.io/api/v1/apps/{}/jwks.json", app_id);
let user_api_url = "https://api.privy.io/v1/users".to_string();
let auth_service = AuthService::new(jwks_url, user_api_url, app_id.clone(), app_secret)
  .with_audience(vec![app_id])
  .with_issuer(vec!["privy.io".to_string()]);

// Add protected routes
let app = Router::new()
  .route("/api/protected/user", get(protected_endpoint))
  .layer(auth_layer(auth_service));
```

### Making Authenticated Requests

Include the bearer token in your requests:

```bash
curl -H "Authorization: Bearer YOUR_JWT_TOKEN" http://localhost:4000/api/protected/user
```

### Error Responses

- `401 Unauthorized`: Missing, invalid, or expired token
- `500 Internal Server Error`: Issues fetching JWKS

Example error response:

```json
{
  "error": "Invalid token: JWT expired"
}
```

## References

- [gel database](https://docs.geldata.com/).
- [nozzle dataset metadata spec](https://www.notion.so/edgeandnode/Dataset-Metadata-Spec-22d8686fc7c280048266ce8f20c00246)
- [nozzle dataset info model](https://whimsical.com/dataset-information-model-Y8fbrdb3dvJnH1BLTG84Wk)
- [nozzle dataset registry design doc](https://www.notion.so/edgeandnode/Nozzle-Data-Registry-API-V1-0-24e8686fc7c280ae9790f3a3aada24b4)
