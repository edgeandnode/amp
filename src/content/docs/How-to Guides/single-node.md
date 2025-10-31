---
title: How to Run Amp in Development Mode
slug: how-to/dev-mode
command: ampd dev
category: local-testing
---

## Goal

Run Amp locally with all components (server, controller, and worker) in a single process.

### When to Use

- Local testing
- CI/CD validation
- Quick prototyping
- Learning Amp

## Steps

1. Start development mode

```bash
ampd dev
```

### Amp starts:

- Query servers (Arrow Flight + JSON Lines)
- Controller (Admin API)
- Embedded worker (`node-id: worker`)

2. Schedule a job via API

```bash
   curl -X POST http://localhost:1610/datasets/eth_mainnet/dump \
    -H "Content-Type: application/json" \
    -d '{"end_block": 1000000}'
```

3. Query the extracted data

```bash
   curl -X POST http://localhost:1603 \
    --data "SELECT COUNT(\*) FROM eth_mainnet.blocks
```

## Expected Outcome

All components run locally and communicate over the metadata DB.

Jobs execute automatically in the embedded worker.

### Limitations

- Not production-ready
- No isolation or scaling
- Shared CPU/memory between services
