Object Store Authentication:
    When using object store URLs, the caller is responsible for providing
    credentials via environment variables (e.g., AWS_ACCESS_KEY_ID,
    GOOGLE_APPLICATION_CREDENTIALS, AZURE_STORAGE_ACCOUNT_NAME).

Examples:
    # Register from local file
    ampctl reg-provider mainnet-rpc ./providers/mainnet.toml

    # Register from S3
    ampctl reg-provider goerli-firehose s3://providers/goerli.toml

    # Register from GCS with custom admin URL
    ampctl reg-provider --admin-url http://localhost:8080 polygon-rpc gs://providers/polygon.toml
