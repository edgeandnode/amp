# Operational Modes

This document describes the operational modes and deployment patterns of the system.

## Overview

Amp provides several commands that can be combined into different deployment patterns:

### Core Commands

1. **`server`** - Query server providing Arrow Flight and JSON Lines interfaces
2. **`worker`** - Standalone worker process for executing scheduled extraction jobs
3. **`controller`** - Controller service providing the Admin API for job management
4. **`migrate`** - Run database migrations on the metadata database

### Operational Modes

Amp supports two primary operational modes:

1. **Single-Node Mode**: Combined controller, server and embedded worker using `ampd dev` for local development and testing
2. **Distributed Mode**: Separate `ampd controller`, `ampd server` and `ampd worker` processes coordinating via metadata DB for production deployments

### Common Deployment Patterns

1. **Server-Only Mode**: Query serving without extraction workers (distributed, read-only)
2. **Controller-Only Mode**: Management interface without query server or workers
3. **Development Mode**: Combined server + embedded worker `ampd dev` (single-node)
4. **Controller + Server + Workers**: Separate server and worker processes coordinating via metadata DB (distributed)

## Distributed Mode

**Distributed mode** separates Amp into distinct controller, server, and worker components that coordinate via a shared metadata database. This architecture enables production deployments with resource isolation, horizontal scaling, and high availability.

### Server Component

#### Purpose

The server component runs Amp as a long-lived query service. The server handles queries while separate worker processes handle extraction and the controller manages jobs. It provides interfaces for querying data but does not execute extraction jobs or provide management APIs.

#### When to Use

- **Production query serving**: Provide query access to extracted data
- **Query-only deployments**: Serve data without running extraction jobs
- **Multi-dataset access**: Provide unified query interface across datasets

#### Architecture

The server provides two query interfaces:

1. **Arrow Flight Server** (default port 1602)
   - High-performance binary query interface
   - Uses Apache Arrow format over gRPC
   - Supports Flight SQL protocol
   - Optimized for large data transfers

2. **JSON Lines Server** (default port 1603)
   - Simple HTTP POST query interface
   - Returns newline-delimited JSON (NDJSON)
   - Supports streaming queries
   - Compression support (gzip, brotli, deflate)

> **Note:** In development mode (`--dev`), the server also includes the Admin API (controller) for convenience.

#### Basic Usage

```bash
# Start query servers (no worker)
ampd server

# Start only specific query interfaces
ampd server --flight-server          # Arrow Flight only
ampd server --jsonl-server           # JSON Lines only
ampd server --flight-server --jsonl-server  # Both query interfaces
```

#### Query Examples

**HTTP JSON Lines:**

```bash
# Simple query
curl -X POST http://localhost:1603 \
  --data "SELECT * FROM 'my_namespace/eth_mainnet'.blocks LIMIT 10"

# Streaming query with compression
curl -X POST http://localhost:1603 \
  -H "Accept-Encoding: gzip" \
  --data "SELECT * FROM 'my_namespace/eth_mainnet'.logs WHERE _block_num > 19000000"
```

**Arrow Flight (Python):**

```python
from pyarrow import flight

client = flight.connect("grpc://localhost:1602")
reader = client.do_get(
    flight.Ticket("SELECT * FROM 'my_namespace/eth_mainnet'.blocks LIMIT 10")
)
table = reader.read_all()
print(table.to_pandas())
```

Without specifying any flags, both query servers are enabled by default.

> [!NOTE]
> The server flags work as explicit selectors, not toggles. When you specify any flags, only those servers are enabled. There is currently no way to disable specific servers while keeping the "default all" behavior. For example:
>
> - `ampd server` → both query servers enabled (Flight + JSON Lines)
> - `ampd server --flight-server` → only Flight server enabled
> - `ampd server --jsonl-server` → only JSON Lines server enabled
>
> To run without a specific server, explicitly list the servers you want.

### Worker Component

#### Purpose

The worker component runs a standalone worker process that executes scheduled dump jobs in **distributed mode** deployments. Workers coordinate with the server via the shared metadata database, enabling distributed extraction architectures.

#### When to Use

- **Production deployments**: Separate compute resources for queries vs extraction
- **Distributed extraction**: Run multiple workers for parallel dataset processing
- **Horizontal scaling**: Add more workers to increase extraction throughput
- **Resource isolation**: Keep heavy extraction workloads separate from query serving
- **High availability**: Workers can fail and restart without affecting queries

#### How It Works

1. Worker registers with metadata DB using provided node ID
2. Maintains heartbeat every 1 second to signal health
3. Listens for job notifications via PostgreSQL LISTEN/NOTIFY
4. Executes assigned dump jobs (pulls data, writes Parquet files)
5. Updates job status and file metadata in database
6. Gracefully resumes jobs on restart
7. Periodically reconciles job state with metadata DB every 60 seconds

#### Basic Usage

```bash
# Start a worker with unique node ID
ampd worker --node-id worker-01

# Multiple workers for distributed processing
ampd worker --node-id worker-01 &
ampd worker --node-id worker-02 &
ampd worker --node-id worker-03 &

# Workers can have descriptive IDs
ampd worker --node-id eu-west-1a-worker
ampd worker --node-id us-east-1b-worker
```

#### Worker Coordination

Multiple workers coordinate through the metadata DB:

- **Job assignment**: Jobs distributed to available workers
- **Health monitoring**: Server tracks worker heartbeats
- **Automatic failover**: Jobs reassigned if worker crashes
- **Load balancing**: Work distributed across active workers

#### Worker Lifecycle

```
1. START → Register with metadata DB
2. HEARTBEAT → Send periodic health signals
3. LISTEN → Wait for job notifications
4. EXECUTE → Process assigned jobs
5. UPDATE → Report progress and results
6. SHUTDOWN → Graceful cleanup (or crash/failover)
```

### Controller Component

#### Purpose

The controller component provides the Admin API for managing Amp operations. It runs as a standalone service separate from the query server, allowing for independent deployment and scaling of management operations.

#### When to Use

- **Production deployments**: Dedicated management interface separate from query serving
- **Secure management**: Deploy controller in a private network while exposing query servers publicly
- **Microservices architecture**: Independent scaling and deployment of management vs query components

#### Architecture

The controller provides the **Admin API Server** (default port 1610):

- RESTful management interface
- Control dump jobs remotely
- Monitor worker status
- Manage datasets and locations
- Query file metadata

#### Basic Usage

```bash
# Start the controller (Admin API)
ampd controller
```

#### Admin API Operations

The Admin API provides full control over the Amp system:

##### Dataset Management

```bash
# List all datasets
curl http://localhost:1610/datasets

# Get dataset details (specific version)
curl http://localhost:1610/datasets/my_namespace/eth_mainnet/versions/1.0.0

# List all versions of a dataset
curl http://localhost:1610/datasets/my_namespace/eth_mainnet/versions

# Register a new dataset
curl -X POST http://localhost:1610/datasets \
  -H "Content-Type: application/json" \
  -d @dataset_definition.json
```

##### Job Control

```bash
# List all jobs
curl http://localhost:1610/jobs

# Deploy a dataset (start a dump job)
curl -X POST http://localhost:1610/datasets/my_namespace/eth_mainnet/versions/1.0.0/deploy \
  -H "Content-Type: application/json" \
  -d '{
    "end_block": 20000000
  }'

# Get job status (replace 42 with actual job_id)
curl http://localhost:1610/jobs/42

# Stop a running job (replace 42 with actual job_id)
curl -X PUT http://localhost:1610/jobs/42/stop

# Delete a job (replace 42 with actual job_id)
curl -X DELETE http://localhost:1610/jobs/42
```

##### Job Control

```bash
# List all jobs
curl http://localhost:1610/jobs

# Start a dump job for a dataset
curl -X POST http://localhost:1610/datasets/eth_mainnet/dump \
  -H "Content-Type: application/json" \
  -d '{
    "end_block": 20000000
  }'

# Get job status (replace 42 with actual job_id)
curl http://localhost:1610/jobs/42

# Stop a running job (replace 42 with actual job_id)
curl -X PUT http://localhost:1610/jobs/42/stop

# Delete a job (replace 42 with actual job_id)
curl -X DELETE http://localhost:1610/jobs/42
```

##### Worker Locations

```bash
# List all registered locations (workers)
curl http://localhost:1610/locations

# Get location details with file statistics (replace 7 with actual location_id)
curl http://localhost:1610/locations/7

# List files at a location (replace 7 with actual location_id)
curl http://localhost:1610/locations/7/files
```

##### File Operations

```bash
# List all files for a dataset
curl http://localhost:1610/files?dataset=my_namespace/eth_mainnet

# Get file metadata (replace 512 with actual file_id)
curl http://localhost:1610/files/512
```

> **Note:** In development mode (`ampd dev`), the controller (Admin API) is automatically included with the server for convenience, so you don't need to run it separately.

## Development Mode _(Single-Node)_

### Purpose

Development mode runs a combined server and worker in a single process for simplified local testing and development. This implements **single-node mode** for local development, where all components run together in a single process. It is activated with the `ampd dev` command.

### When to Use

- **Local development**: Quick testing without separate worker processes
- **CI/CD pipelines**: Simplified testing in automated environments
- **Quick prototyping**: Rapid experimentation with datasets and queries
- **Learning and exploration**: Understanding Amp behavior without complex setup
- **❌ Not for production**: Lacks separation of concerns and fault isolation

### How It Works

When running `ampd dev`:

1. Server starts both query interfaces (Arrow Flight, JSON Lines)
2. Controller (Admin API) automatically starts in the same process
3. Worker automatically spawns in the same process with node ID "worker"
4. Worker registers with metadata DB and begins listening for jobs
5. Jobs can be scheduled via Admin API and execute within the same process
6. Simplified logging and error reporting for easier debugging

### Basic Usage

```bash
# Start development mode
ampd dev

# Schedule a job via Admin API (executed by embedded worker)
curl -X POST http://localhost:1610/datasets/my_namespace/eth_mainnet/versions/dev/deploy \
  -H "Content-Type: application/json" \
  -d '{
    "end_block": 1000000
  }'

# Query the data as it's being extracted
curl -X POST http://localhost:1603 \
  --data "SELECT COUNT(*) FROM 'my_namespace/eth_mainnet'.blocks"
```

### Benefits

- **Single process**: No need to manage multiple processes
- **Simplified setup**: No separate worker configuration required
- **Fast iteration**: Quick start/stop cycles for testing
- **Complete workflow**: Test entire extract-query pipeline locally

### Limitations

- **No fault isolation**: Worker crash brings down query server
- **Resource contention**: Extraction competes with queries for CPU/memory
- **No horizontal scaling**: Cannot add more workers
- **No high availability**: Single point of failure
- **Not production-ready**: Lacks robustness for production workloads

## Deployment Patterns

This section describes common deployment topologies and when to use each.

### Pattern 1: Development Mode _(Single-Node)_

```
┌──────────────────────────────────────────┐
│ ampd dev                               │
│ ┌──────────────┐ ┌────────────────────┐  │
│ │Server        │ │ Controller         │  │
│ │- Flight      │ │ - Admin API        │  │
│ │- JSON Lines  │ │                    │  │
│ └──────────────┘ └────────────────────┘  │
│ ┌──────────────┐                         │
│ │ Worker       │                         │
│ │ (embedded)   │                         │
│ └──────────────┘                         │
└──────────────────────────────────────────┘
    │
    ├─ PostgreSQL (metadata)
    └─ Object Store (parquet files)
```

**When to use:**

- Local development and testing
- CI/CD pipelines
- Quick prototyping
- **Not suitable for production deployments**

**Operational mode:** Single-Node

**Commands:**

```bash
ampd dev
```

### Pattern 2: Query-Only Server _(Distributed, Read-Only)_

```
┌─────────────────────┐
│ ampd server       │
│ ┌─────────────────┐ │
│ │Server           │ │
│ │- Flight         │ │
│ │- JSON Lines     │ │
│ └─────────────────┘ │
└─────────────────────┘
   │
   ├─ PostgreSQL (metadata)
   └─ Object Store (read-only)
```

**When to use:**

- Read-only query serving
- Separation of concerns (queries vs extraction)
- Datasets populated by external extraction processes
- Multiple query replicas for load balancing

**Operational mode:** Distributed (query component only)

**Commands:**

```bash
ampd server
```

### Pattern 3: Server + Controller + Workers _(Distributed)_

```
┌────────────────────┐   ┌──────────────────┐
│ampd server       │   │ampd controller │
│┌──────────────────┐│   │┌────────────────┐│
││Server            ││   ││Controller      ││
││- Flight          ││   ││- Admin API     ││
││- JSON Lines      ││   │└────────────────┘│
│└──────────────────┘│   └──────────────────┘
└────────────────────┘            │
         │                        │
         │               ┌──────────────────┐
         │               │ampd worker     │
         │               │┌────────────────┐│
         │               ││Worker-1        ││
         │               │└────────────────┘│
         │               └──────────────────┘
         │               ┌──────────────────┐
         │               │ampd worker     │
         │               │┌────────────────┐│
         │               ││Worker-2        ││
         │               │└────────────────┘│
         │               └──────────────────┘
         │                      │
         └──────────────────────┘
         │
         ├─ PostgreSQL (metadata, coordination)
         └─ Object Store (parquet files)
```

**When to use:**

- Production deployments
- Resource isolation (CPU/memory for queries vs extraction)
- Horizontal scaling of extraction
- High availability (workers can fail independently)
- Geographic distribution of workers

**Operational mode:** Distributed

**Commands:**

```bash
# Server node
ampd server

# Controller node
ampd controller

# Worker nodes (multiple)
ampd worker --node-id worker-01
ampd worker --node-id worker-02
ampd worker --node-id worker-03
```

### Pattern 4: Distributed Multi-Region _(Distributed)_

```
Region A                      Region B
┌────────────────────┐        ┌────────────────────┐
│ampd server       │        │ampd server       │
│┌──────────────────┐│        │┌──────────────────┐│
││Server            ││        ││Server            ││
││- Flight          ││        ││- Flight          ││
││- JSON Lines      ││        ││- JSON Lines      ││
│└──────────────────┘│        │└──────────────────┘│
└────────────────────┘        └────────────────────┘
         │                             │
┌────────────────────┐        ┌────────────────────┐
│ampd controller   │        │ampd worker       │
│┌──────────────────┐│        │┌──────────────────┐│
││Controller        ││        ││Worker            ││
││- Admin API       ││        ││Region-B          ││
│└──────────────────┘│        │└──────────────────┘│
└────────────────────┘        └────────────────────┘
         │                             │
┌────────────────────┐                 │
│ampd worker       │                 │
│┌──────────────────┐│                 │
││Worker            ││                 │
││Region-A          ││                 │
│└──────────────────┘│                 │
└────────────────────┘                 │
         │                             │
         └─────────────────────────────┘
         │
         ├─ PostgreSQL (shared metadata)
         └─ Object Store (shared parquet files)
```

**When to use:**

- Global deployments with low-latency requirements
- Geographic redundancy
- Load distribution across regions
- Large-scale production systems

**Operational mode:** Distributed

**Commands:**

```bash
# Region A
ampd server
ampd controller
ampd worker --node-id us-east-1-worker

# Region B
ampd server
ampd worker --node-id eu-west-1-worker
```

## Choosing Between Modes

### Use Single-Node Mode (Development) When:

- Local development
- Quick prototyping
- Testing full workflow
- Learning Amp capabilities
- ❌ **Not for production deployments**

### Use Distributed Mode When:

**Query-only server:**

- Read-only query serving
- Datasets populated by external extraction processes
- Multiple query replicas needed
- Separating read from write workloads

**Controller-only:**

- Management interface without query serving
- Secure management in private network
- Job scheduling and monitoring
- Datasets managed by external processes

**Controller + Server + Workers (full distributed):**

- Production deployments
- Resource isolation needed
- Horizontal scaling required
- High availability important
- Continuous data ingestion
- Multi-region deployments
- Independent scaling of management, query, and extraction components

## Scaling Path

**Recommended progression for growing deployments:**

### Stage 1: Development & Testing

- **Mode:** Single-Node
- Use `ampd dev` for local development and testing
- Single machine, minimal setup
- **Not for production use**

### Stage 2: Production Single-Region

- **Mode:** Distributed
- Deploy `ampd controller` on management node
- Deploy `ampd server` on query node(s)
- Deploy `ampd worker --node-id <id>` on extraction node(s)
- Enable observability (OpenTelemetry)
- Configure compaction
- Production-ready with resource isolation

### Stage 3: Scaled Distributed Extraction

- **Mode:** Distributed (scaled)
- Deploy `ampd controller` for centralized management
- Deploy multiple `ampd server` instances for query load balancing
- Deploy multiple `ampd worker` instances for parallel extraction
- Shared PostgreSQL and object store
- Horizontal scaling for management, queries, and extraction

### Stage 4: Multi-Region Production

- **Mode:** Distributed (global)
- Deploy `ampd controller` in primary region for centralized management
- Deploy multiple `ampd server` instances in different regions for low-latency queries
- Deploy `ampd worker` instances near data sources
- Global shared metadata DB and object store
- Full observability and monitoring

### Mixing Modes

Different operational modes can coexist in the same deployment:

- Run **distributed mode** (controller + server + workers) for continuous ingestion and queries
- Deploy query-only servers (distributed, read-only) in regions without extraction needs
- Deploy controller-only for management in secure/private networks

## Security Considerations

Understanding the security implications of each component is critical for production deployments. The separation of controller, server, and worker components enables fine-grained security controls through network isolation and access restrictions.

### Component Security Profiles

Each Amp component has different security characteristics and requirements:

#### Controller (Admin API - Port 1610)

**Security Level:** Most sensitive - requires strictest controls

The controller provides administrative capabilities and should be treated as a privileged management interface:

- **Capabilities:**
  - Job scheduling and control (start, stop, delete jobs)
  - Dataset registration and modification
  - Worker management and monitoring
  - File metadata access and manipulation

- **Security Requirements:**
  - **MUST** be deployed in a private network
  - **MUST NOT** be exposed to public internet
  - Access should be restricted to authorized operators only
  - Consider deploying behind VPN or bastion host
  - Implement strict firewall rules limiting source IPs

- **Network Isolation:**
  - Place in management/admin subnet/VPC
  - Restrict database access to management operations
  - Use internal load balancers only (no public IPs)

#### Server (Query Interfaces - Ports 1602, 1603)

**Security Level:** Public-facing - read-only with rate limiting

The server provides query access and can be safely exposed to end users:

- **Capabilities:**
  - Arrow Flight queries (port 1602)
  - JSON Lines queries (port 1603)
  - Read-only access to extracted data
  - No administrative or modification capabilities

- **Security Requirements:**
  - Can be exposed to public internet
  - Implement rate limiting and query timeouts
  - Monitor for query abuse (resource exhaustion)
  - Consider request authentication via reverse proxy/API gateway
  - Database connection should use read-only credentials if possible

- **Network Isolation:**
  - Place in public subnet/DMZ
  - Can use public load balancers
  - Separate from controller and worker networks

#### Worker (No Exposed Ports)

**Security Level:** Internal component - trusted environment required

Workers execute extraction jobs and have no network-facing interfaces:

- **Capabilities:**
  - Execute extraction jobs
  - Write data to object store
  - Update job status in metadata database
  - No exposed network services

- **Security Requirements:**
  - Must have database write access
  - Must have object store write access
  - Should run in trusted/internal network
  - Credentials for blockchain data sources (RPC, Firehose)
  - No inbound network access required

- **Network Isolation:**
  - Place in private worker subnet
  - Only outbound connections (database, object store, data sources)
  - No public IP addresses needed

### Authentication & Authorization

**Current State:**

Amp components currently do **not include built-in authentication or authorization** mechanisms. All network-based security relies on:

1. **Network isolation** (private networks, firewalls, VPCs)
2. **Database authentication** (PostgreSQL user/password)
3. **Object store authentication** (AWS credentials, GCS service accounts)

**Recommended External Security Layers:**

For production deployments requiring authentication:

1. **API Gateway / Reverse Proxy**

   ```
   Client → API Gateway (Auth) → Amp Server
   ```

   - Use Kong, Nginx, Traefik, or cloud API gateways
   - Implement API key authentication
   - JWT token validation
   - OAuth2/OIDC integration

2. **Mutual TLS (mTLS)**

   ```
   Client (with cert) ←TLS→ mTLS Proxy → Amp Server
   ```

   - Terminate TLS at proxy/load balancer
   - Client certificate validation
   - Especially suitable for Arrow Flight (gRPC)

3. **VPN / Zero Trust Network**
   - WireGuard, Tailscale, or cloud VPN
   - Controller access should always be behind VPN
   - Optional for query servers depending on data sensitivity

4. **Network Policy Enforcement**
   - Kubernetes NetworkPolicies
   - AWS Security Groups
   - GCP Firewall Rules
   - Azure NSGs

### Secrets Management

**Required Secrets:**

- PostgreSQL connection strings
- Object store credentials (AWS_SECRET_ACCESS_KEY, GCS service account JSON)
- Blockchain RPC API keys (if using authenticated endpoints)
- Firehose authentication tokens

**Recommendations:**

1. **Never commit secrets to version control**
2. **Use secret management systems:**
   - Kubernetes Secrets
   - AWS Secrets Manager
   - Google Cloud Secret Manager
   - HashiCorp Vault
   - Azure Key Vault

3. **Environment variables via secret injection:**

   ```bash
   # Bad - hardcoded in config
   AMP_CONFIG_METADATA_DB_URL=postgresql://user:password@host/db

   # Good - injected from secret manager
   AMP_CONFIG_METADATA_DB_URL=$(kubectl get secret db-creds -o jsonpath='{.data.url}' | base64 -d)
   ```

4. **Rotate credentials regularly**
5. **Use IAM roles / service accounts where possible** (avoid static credentials)

### Threat Model Summary

| Component  | Threat Level | Attack Surface          | Mitigation                                          |
| ---------- | ------------ | ----------------------- | --------------------------------------------------- |
| Controller | **HIGH**     | Admin API (1610)        | Private network, VPN access, audit logging          |
| Server     | **MEDIUM**   | Query APIs (1602, 1603) | Rate limiting, read-only DB access, DDoS protection |
| Worker     | **LOW**      | None (internal)         | Private network, minimal outbound access            |
| Dev Mode   | **CRITICAL** | All services exposed    | **Never use in production**                         |

## See Also

- [Configuration Guide](config.md) - Detailed configuration options
- [Upgrading Guide](upgrading.md) - Upgrading Amp between versions
- [Dataset Definitions](datasets.md) - How to define datasets
- [Query Guide](queries.md) - Writing SQL queries with custom UDFs
- [Deployment Guide](deployment.md) - Production deployment patterns
