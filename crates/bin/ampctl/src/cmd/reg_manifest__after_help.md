Object Store Authentication:
    When using object store URLs, the caller is responsible for providing
    credentials via environment variables (e.g., AWS_ACCESS_KEY_ID,
    GOOGLE_APPLICATION_CREDENTIALS, AZURE_STORAGE_ACCOUNT_NAME).

Examples:
    # Register from local file
    ampctl reg-manifest edgeandnode/eth_mainnet@1.0.0 ./manifests/eth.json

    # Register from S3
    ampctl reg-manifest edgeandnode/eth_mainnet@1.0.0 s3://manifests/eth.json

    # Register from GCS with custom admin URL
    ampctl reg-manifest --admin-url http://localhost:8080 graph/eth_mainnet@1.0.0 gs://manifests/eth.json
