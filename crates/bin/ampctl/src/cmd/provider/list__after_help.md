Output Format:
    The provider list is returned as a JSON array containing all provider
    configurations. You can pipe the output to jq or other tools for filtering
    and processing.

Examples:
    # List all providers
    ampctl provider ls

    # Alternative command name
    ampctl provider list

    # Use with custom admin URL
    ampctl provider ls --admin-url http://production:1610

    # Filter providers with jq
    ampctl provider ls | jq '.[] | select(.kind == "evm-rpc")'

    # Count providers by kind
    ampctl provider ls | jq 'group_by(.kind) | map({kind: .[0].kind, count: length})'

    # Extract provider names
    ampctl provider ls | jq '.[].name'
