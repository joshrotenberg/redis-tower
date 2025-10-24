# Redis Cluster Setup with Docker Compose

This guide explains how to set up and use the Redis cluster for testing redis-tower's cluster support.

## Cluster Architecture

The cluster consists of:
- **3 Master nodes**: ports 7000, 7001, 7002
- **3 Replica nodes**: ports 7003, 7004, 7005
- **Slot distribution**: 16384 slots split across 3 masters
  - Master 1 (7000): slots ~0-5460
  - Master 2 (7001): slots ~5461-10922
  - Master 3 (7002): slots ~10923-16383

## Quick Start

### 1. Start the Cluster

```bash
# Start all Redis nodes and initialize the cluster
docker-compose up -d

# Wait for cluster initialization (about 15 seconds)
# The cluster-init service will automatically create the cluster

# Check cluster status
docker-compose logs cluster-init
```

### 2. Verify Cluster is Ready

```bash
# Connect to any master node
docker exec -it redis-cluster-1 redis-cli -p 7000

# Inside redis-cli:
> CLUSTER INFO
> CLUSTER NODES
> CLUSTER SLOTS
```

You should see:
- `cluster_state:ok`
- All 6 nodes listed
- Slot assignments

### 3. Test Cluster Operations

```bash
# Set a key (will be routed to correct node based on slot)
docker exec -it redis-cluster-1 redis-cli -c -p 7000 SET user:123 "Alice"

# Get the key
docker exec -it redis-cluster-1 redis-cli -c -p 7000 GET user:123

# The -c flag enables cluster mode (follows redirects)
```

## Cluster Behavior

### Slot Calculation

Redis uses CRC16 to calculate which slot (0-16383) a key belongs to:

```
slot = CRC16(key) mod 16384
```

For keys with hash tags: `{user}:123` and `{user}:456` both go to the same slot (based on "user").

### MOVED Redirects

When you send a command to the wrong node:

```
127.0.0.1:7000> GET mykey
(error) MOVED 12539 127.0.0.1:7002
```

The client should:
1. Update its slot mapping
2. Reconnect to the correct node
3. Retry the command

### ASK Redirects

During slot migration (resharding):

```
127.0.0.1:7000> GET mykey
(error) ASK 12539 127.0.0.1:7002
```

The client should:
1. Send ASKING to the target node
2. Retry the command
3. NOT update slot mapping (migration is temporary)

## Useful Commands

### Check Cluster Health

```bash
# Cluster info
redis-cli -h localhost -p 7000 CLUSTER INFO

# Node list with slot assignments
redis-cli -h localhost -p 7000 CLUSTER NODES

# Slot distribution
redis-cli -h localhost -p 7000 CLUSTER SLOTS
```

### Manual Key Routing

```bash
# Find which slot a key belongs to
redis-cli -h localhost -p 7000 CLUSTER KEYSLOT "user:123"

# Find which node serves a slot
redis-cli -h localhost -p 7000 CLUSTER NODES | grep <slot-number>
```

### Reset Cluster (Clean Start)

```bash
# Stop and remove all containers and volumes
docker-compose down -v

# Start fresh
docker-compose up -d
```

## Standalone Redis

The docker-compose also includes a standalone Redis instance for non-cluster examples:

```bash
# Connect to standalone Redis
docker exec -it redis-standalone redis-cli

# Or from host
redis-cli -h localhost -p 6379
```

## Testing Failover

### Simulate Master Failure

```bash
# Stop a master node
docker stop redis-cluster-1

# Check cluster status - a replica should be promoted
docker exec -it redis-cluster-2 redis-cli -p 7001 CLUSTER NODES

# Restart the node
docker start redis-cluster-1

# It will rejoin as a replica
```

## Cluster Client Requirements

For redis-tower's cluster client to work, it needs to:

1. **Discover topology**: Use `CLUSTER SLOTS` or `CLUSTER NODES`
2. **Calculate slots**: Implement CRC16(key) mod 16384
3. **Route commands**: Send to correct node based on slot
4. **Handle MOVED**: Update slot map, retry on correct node
5. **Handle ASK**: Send ASKING, retry (no slot map update)
6. **Connection pool**: Maintain connections to all master nodes
7. **Refresh topology**: Periodically update slot mappings

## Example Usage with redis-tower

```rust
use redis_tower::cluster::ClusterClient;

let client = ClusterClient::new(vec![
    "127.0.0.1:7000",
    "127.0.0.1:7001",
    "127.0.0.1:7002",
]).await?;

// Client automatically:
// - Discovers all nodes
// - Maps slots to nodes
// - Routes commands correctly
// - Handles redirects

client.set("user:123", "Alice").await?;
let value = client.get("user:123").await?;
```

## Troubleshooting

### Cluster won't initialize

```bash
# Check if all nodes are running
docker-compose ps

# Check logs for errors
docker-compose logs redis-cluster-1
docker-compose logs cluster-init
```

### Connection refused

Make sure you're using the host network addresses:
- From host: `127.0.0.1:7000` (or `localhost:7000`)
- From container: `redis-cluster-1:7000`

### Slots not covered

```bash
# This means cluster initialization failed
# Reset and try again:
docker-compose down -v
docker-compose up -d
```

## Cleanup

```bash
# Stop all services
docker-compose down

# Stop and remove volumes (clean slate)
docker-compose down -v
```
