## Examples

Stop a job:
  $ ampctl job stop 12345

With custom admin URL:
  $ ampctl job stop 12345 --admin-url http://prod-server:1610

## Notes

Stopping a job is a request, not immediate termination. The worker node will
attempt to finish current operations cleanly. Check job status with
`ampctl job inspect <id>` to confirm it stopped.
