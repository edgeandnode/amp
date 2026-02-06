---
title: Install Amp Using `ampup`
description: Complete guide to installing Amp on Ubuntu/Debian using the ampup installation tool
tags: [installation, getting-started, ampup, quickstart]
slug: /guides/install-ampup
# Content Classification
type: how-to
audience: developers
skill_level: beginner
# SEO & Discovery
keywords:
  - amp installation
  - ampup install
  - amp setup ubuntu
  - amp docker postgres
  - ampctl dataset deploy
canonical: /docs/guides/install-ampup
---

# How-to Guide: Install Amp Using ampup

> **Platform:** Ubuntu 20.04+ | Debian 11+  
> **Time Required:** 30-45 minutes  
> **Skill Level:** Beginner  

**In this guide:**
- Install system dependencies and Docker
- Set up `ampup` and Amp components
- Configure PostgreSQL database
- Deploy and query your first dataset

---

# Get Started 

## Prerequisite

- Ubuntu/Debian-based Linux 
- sudo/root access

## 1. Install System Dependencies 

These tools are required to compile and build Amp from source.

Install the dependencies for your operating system:

**Debian/Ubuntu**

```bash 
sudo apt update -y
sudo apt install -y cmake build-essential pkg-config libssl-dev curl
```

**macOS (Homebrew)**
```bash
brew install cmake pkg-config openssl curl
```

Requires Xcode Command Line Tools:
```bash
xcode-select --install
```

**Arch Linux (pacman)**
```bash
sudo pacman -S --needed cmake base-devel pkgconf openssl curl
```

**Red Hat-based Linux (rpm)**

```bash
sudo yum install -y cmake gcc gcc-c++ make pkgconfig openssl-devel curl
```

**Fedora Linux (dnf)**
```bash
sudo dnf install -y cmake gcc gcc-c++ make pkg-config openssl-devel curl
```

**What this does**: Updates your package list and installs compilation tools and security libraries.
--- 

## 2. Install Docker

Install Docker

1. Follow Docker’s official installation guide for your OS: 
https://docs.docker.com/engine/install

2. Linux users: complete Docker’s post-install steps
https://docs.docker.com/engine/install/linux-postinstall/

Verify:

```bash
docker run hello-world
```

---

## 3. Install `ampup`

The `ampup` tool is the easiest and fastest way to install Amp. By default, it downloads prebuilt binaries, so you don’t need to build anything from source.

Install `ampup` (includes `ampd` and `ampctl`):

```bash
curl --proto '=https' --tlsv1.2 -sSf https://ampup.sh/install | sh
source ~/.bashrc
```

**What this does**: Downloads and installs the Amp tooling and adds it to your PATH..


Install Amp (prebuilt binaries):

```bash
ampup install
```

**What this does**: Downloads and installs the latest precompiled Amp binaries for your system. This is the recommended approach for beginners and most users. You do not need to build Amp from source unless you are developing Amp itself or testing unreleased changes.

(Optional) Build Amp from source:

Only required if you are contributing to Amp or need a custom build.

```bash
ampup build
```

**What this does**: Compiles Amp from source. 

---

## 4. Configure PostgresSQL Database 

Amp uses PostgreSQL to store data. You'll run it in a Docker container.

Create Project Directory

```bash
mkdir -p amp/configuration/{data,providers,manifests}
cd amp/configuration
```

**What this does**: Creates an organized folder structure:

- `amp/configuration/` - Main configuration folder
- `data/` - Data storage
- `providers/` - Provider configurations
- `manifests/` - Dataset manifests

### Create Database Configuration

Create `docker-compose.yaml` file:

```bash
cat <<'EOF' > docker-compose.yaml
services:
  # PostgreSQL database at postgresql://postgres:postgres@localhost:5432/amp
  db:
    image: postgres:alpine
    environment:
      - POSTGRES_USER=postgres
      - POSTGRES_PASSWORD=postgres
      - PGUSER=postgres
      - POSTGRES_DB=amp
    ports:
      - '5432:5432'
  
  # Database explorer at http://localhost:7402/?pgsql=db&username=postgres&db=amp&ns=public
  # Password: postgres
  adminer:
    image: adminer
    restart: always
    ports:
      - 7402:8080
  
  # Observability stack (Grafana + OTEL + Loki + Tempo + Prometheus + Pyroscope)
  # Access Grafana UI at http://localhost:3000
  lgtm:
    image: grafana/otel-lgtm
    ports:
      - "3000:3000" # Grafana UI
      - "4317:4317" # OTLP gRPC
      - "4318:4318" # OTLP HTTP
    volumes:
      - ./grafana/dashboards:/var/lib/grafana/dashboards
      - ./grafana/provisioning/dashboards:/otel-lgtm/grafana/conf/provisioning/dashboards
EOF
```

What each service does:

- `db`: PostgreSQL database where Amp stores datasets
- `adminer`: Web-based database browser (optional, helpful for beginners)
- `lgtm`: Monitoring dashboard (optional, for advanced users)

If you have PostgreSQL already installed, stop it to avoid conflicts:

```bash
sudo systemctl stop postgresql
sudo systemctl disable postgresql
```

> Note: This only affects system-wide PostgreSQL. Our Docker version runs independently.

Start Docker services:

```bash
docker compose up -d
```

**What this does**: Starts PostgreSQL and other services in the background (-d means "detached").

---
## 5. Configure Environment Variables

Tell Amp where to find its configuration file:

```bash
export AMP_CONFIG="$(pwd)/config.toml"
```

**Important for beginners**: This only works in your current terminal session. To make it permanent, add it to your `~/.bashrc`:

Verify the configuration:

```bash
ampd --help
```

The configuration file path should appear in the help output.

---

## 6. Start Amp Server

Now start the Amp Server

```bash 
ampd --config ./config.toml server
```

**What this does:** Starts the main Amp server process. Leave this terminal window open.

### Troubleshooting:

- Config file not found: Ensure you're in the amp/configuration/ directory or use the full path.
- Port already in use: Check if another Amp instance is running with ps aux | grep ampd.

---

## 7. Deploy a Dataset (Optional Example: EVM RPC)

This step demonstrates deploying an Ethereum mainnet RPC dataset. Open a new terminal window (keep the server running in the first).

Navigate to configuration directory:

```bash
cd ~/amp/configuration
export AMP_CONFIG="$(pwd)/config.toml"
```

Generate a manifest:

```bash 
ampctl manifest generate \
  --kind evm-rpc \
  --network mainnet \
  --out ./configuration/.manifests/evm_rpc.json
```

Get the dataset hash: 

```bash
ampctl dataset versions _/evm_rpc
```

Potential output example:

```
Dataset: _/evm_rpc
Dev hash: <HASH>
```

Copy this hash. You will need it in the next step. 

Deploy the dataset (replace <HASH> with the hash from above):

```bash
ampctl dataset deploy _/evm_rpc@<HASH>
```

**What this does**: Registers your dataset with the Amp server.
---

## 8. Test Your Install

### Start development mode
In a new terminal (or stop the server from Step 6 first):

```bash
cd ~/amp/configuration
export AMP_CONFIG="$(pwd)/config.toml"
ampd dev --admin-server --flight-server --jsonl-server
```

**What this does**: Runs Amp in development mode with additional API servers for testing.

### Query Your Dataset
Open another terminal and test with a SQL query: 

```bash
curl -X POST http://localhost:1603 \
  --data 'select * from "_/evm_rpc@<HASH>".blocks limit 1'
```
> Replace <HASH> with your dataset hash.
---
### Troubleshooting:

- Connection refused: Ensure the dev server is running (Step 8).
- Dataset not found: Verify you deployed the dataset (Step 7) and used the correct hash.
- Empty result: The dataset may still be syncing. Wait 1-2 minutes and try again.