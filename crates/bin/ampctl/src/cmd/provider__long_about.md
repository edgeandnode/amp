The provider command provides management for provider configurations.

Providers define connection details for external data sources such as EVM RPC endpoints,
Firehose streams, and other blockchain data providers. Each provider configuration includes:

- Provider name: Unique identifier (e.g., mainnet-rpc, polygon-firehose)
- Provider kind: Type of provider (evm-rpc, firehose, etc.)
- Network: Blockchain network (mainnet, goerli, polygon, etc.)
- Additional configuration: Provider-specific settings (endpoints, auth, etc.)

Provider configurations are stored as TOML files and managed through the admin API.
