## Examples

List first 50 jobs (default):
  $ ampctl job list

Using the `ls` alias:
  $ ampctl job ls

List first 100 jobs:
  $ ampctl job list --limit 100

Paginate - get next 50 jobs after ID 1234:
  $ ampctl job list --after 1234

Use short aliases:
  $ ampctl job list -l 20 -a 500

Use jq to filter completed jobs:
  $ ampctl job list | jq '.jobs[] | select(.status == "COMPLETED")'
