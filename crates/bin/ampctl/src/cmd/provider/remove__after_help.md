Deletion Safety:
    Deleting a provider removes its configuration from storage. Any datasets
    currently using this provider may fail until a new provider is configured.

    Consider checking which datasets depend on a provider before deletion.

Examples:
    # Remove a provider by name
    ampctl provider rm mainnet-rpc

    # Alternative using the "remove" alias
    ampctl provider remove polygon-rpc

    # Use with custom admin URL
    ampctl provider rm --admin-url http://production:1610 goerli-rpc

    # Remove after verification
    ampctl provider inspect old-provider && ampctl provider rm old-provider
