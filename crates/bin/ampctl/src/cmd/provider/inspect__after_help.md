Output Format:
    The provider configuration is retrieved and printed as pretty-formatted JSON
    to stdout. You can pipe the output to jq or other tools for further processing.

Examples:
    # Inspect a provider by name
    ampctl provider inspect mainnet-rpc

    # Save provider configuration to a file
    ampctl provider inspect polygon-rpc > polygon-config.json

    # Extract specific fields with jq
    ampctl provider inspect mainnet-rpc | jq '.kind'

    # Use with custom admin URL
    ampctl provider inspect --admin-url http://production:1610 mainnet-rpc

    # Check provider endpoint
    ampctl provider inspect mainnet-rpc | jq '.endpoint'
