Output Format:
    The manifest is retrieved from content-addressable storage and printed
    as pretty-formatted JSON to stdout. You can pipe the output to jq or
    other tools for further processing.

Examples:
    # Inspect a manifest by hash (64-character hex string)
    ampctl manifest inspect 1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef

    # Save manifest to a file
    ampctl manifest inspect abcd1234ef567890abcd1234ef567890abcd1234ef567890abcd1234ef567890 > manifest.json

    # Extract specific fields with jq
    ampctl manifest inspect abcd1234ef567890abcd1234ef567890abcd1234ef567890abcd1234ef567890 | jq '.name'

    # Use with custom admin URL
    ampctl manifest inspect --admin-url http://production:1610 abcd1234ef567890abcd1234ef567890abcd1234ef567890abcd1234ef567890

    # Verify manifest hash matches content
    ampctl manifest inspect abcd1234ef567890abcd1234ef567890abcd1234ef567890abcd1234ef567890 | sha256sum
