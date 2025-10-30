---
title: Quickstart Locally
description: Build with Amp locally
---

This quickstart helps you jump start Amp locally

## Prerequisites

1. Repository Setup

```
git clone <repository-url>
cd <repository-name>`
```

2. PostgreSQL database setup

You need this database to store metadata.  
The easiest way to set this up is by using **Docker**.

### Steps

1. From the root of the `amp` repository, start the PostgreSQL service:

```bash
   docker-compose up -d db
```

This command starts PostgreSQL in the background.

2. Verify that the PostgreSQL container is running:

```bash
docker ps | grep postgres
```

## Step-by-Step

1.  Navigate to the amp repository

`cd /path/to/amp`

2. Build the release binary

`cargo build --release -p ampd`

3. Make it available in our PATH (choose one method):

### Method A: Create a symlink in a directory already in PATH

`sudo ln -sf $(pwd)/target/release/ampd /usr/local/bin/ampd`

### Method B: Add the target directory to our PATH

`export PATH="$(pwd)/target/release:$PATH"`

### Add this to ~/.bashrc, ~/.zshenv, etc. to persist

4. Verify installation:

`ampd --version`
