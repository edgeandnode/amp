Object Store Authentication:
    When using object store URLs, the caller is responsible for providing
    credentials via environment variables (e.g., AWS_ACCESS_KEY_ID,
    GOOGLE_APPLICATION_CREDENTIALS, AZURE_STORAGE_ACCOUNT_NAME).

Content-Addressable Storage:
    The manifest is validated, canonicalized, and stored with a hash computed
    from its canonical JSON representation. The same manifest content always
    produces the same hash, enabling deduplication and integrity verification.

Examples:
    # Register from local file
    ampctl manifest register ./manifests/eth_mainnet.json

    # Register from S3
    ampctl manifest register s3://my-bucket/manifests/eth.json

    # Register from GCS with custom admin URL
    ampctl manifest register --admin-url http://localhost:8080 gs://manifests/polygon.json

    # Save the returned hash for later use
    HASH=$(ampctl manifest register ./manifest.json | grep "Manifest hash:" | awk '{print $3}')
