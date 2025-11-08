Worker management commands provide visibility into the distributed worker nodes that execute extraction and processing jobs.

Workers are the execution units in Amp's distributed architecture. Each worker:
- Registers with the system on startup
- Sends periodic heartbeats to indicate liveness
- Executes jobs assigned by the scheduler
- Reports version and build information

Use these commands to monitor worker health, track versions, and troubleshoot distributed operations.
