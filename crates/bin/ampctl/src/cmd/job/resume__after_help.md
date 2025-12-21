## Examples

Resume a job:
  $ ampctl job resume 12345

With custom admin URL:
  $ ampctl job resume 12345 --admin-url http://prod-server:1610

## Notes

Resuming a job is a request, not immediate start. The worker node will
attempt to finish current operations cleanly. Check job status with
`ampctl job inspect <id>` to confirm it resumed.