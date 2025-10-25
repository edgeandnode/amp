Deletion Safety:
    Manifests that are linked to one or more dataset versions CANNOT be deleted.
    The API will return a 409 Conflict error if you attempt to delete a linked manifest.

    To delete a linked manifest, you must first remove all dataset version associations.

Idempotent Operation:
    Deleting a non-existent manifest is treated as success (returns 204 No Content).
    This makes the operation safe to retry without checking if the manifest exists.

Examples:
    # Remove a manifest by hash (64-character hex string)
    ampctl manifest rm 1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef

    # Alternative using the "remove" alias
    ampctl manifest remove abcd1234ef567890abcd1234ef567890abcd1234ef567890abcd1234ef567890

    # Use with custom admin URL
    ampctl manifest rm --admin-url http://production:1610 abcd1234ef567890abcd1234ef567890abcd1234ef567890abcd1234ef567890

    # Attempt to delete (will fail with 409 if linked to datasets)
    ampctl manifest rm abcd1234ef567890abcd1234ef567890abcd1234ef567890abcd1234ef567890 || echo "Manifest is still in use"
