# Cluster Tests Currently Skipped

Cluster integration tests are temporarily disabled due to Docker networking limitations.

## Issue
When running tests from the host machine, CLUSTER SLOTS returns internal Docker IPs that aren't routable from the host.

## TODO
- Option 1: Run tests inside Docker containers
- Option 2: Fix docker-wrapper announce-ip propagation
- Option 3: Use a different cluster testing approach

For now, focus on non-cluster integration tests.
