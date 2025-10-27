Object Store Authentication:
    When using object store URLs, the caller is responsible for providing
    credentials via environment variables (e.g., AWS_ACCESS_KEY_ID,
    GOOGLE_APPLICATION_CREDENTIALS, AZURE_STORAGE_ACCOUNT_NAME).

Provider Configuration Format:
    Provider configurations are stored as TOML files with the following structure:

    ```toml
    kind = "evm-rpc"
    network = "mainnet"
    # Additional provider-specific configuration fields
    endpoint = "https://ethereum-rpc.example.com"
    ```

Examples:
    # Register from local file
    ampctl provider register mainnet-rpc ./providers/mainnet.toml

    # Register from S3
    ampctl provider register polygon-rpc s3://my-bucket/providers/polygon.toml

    # Register from GCS with custom admin URL
    ampctl provider register --admin-url http://localhost:8080 goerli-rpc gs://providers/goerli.toml

    # Register from Azure
    ampctl provider register arbitrum-rpc az://container/providers/arbitrum.toml
