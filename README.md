# Reddish

Reddish is a Redis-compatible server implementation built as a playful take on the original Redis. It supports core Redis functionality including key-value operations, data persistence with RDB files, primary-replica replication, and advanced data types like lists and streams, all while maintaining compatibility with standard Redis clients.

## Features

### 🔑 Key-Value Operations

- **SET/GET**: Basic key-value storage with optional expiration
- **Automatic expiry**: Keys automatically expire based on TTL
- **KEYS**: List all keys (supports "\*" pattern)

### 💾 Data Persistence

- **RDB file format**: Load and save data in Redis-compatible RDB format
- **Expiry preservation**: Timestamps are preserved across restarts
- **Configurable paths**: Custom directory and filename support

### 🔄 Replication

- **Primary-replica setup**: Full master-slave replication support
- **Command propagation**: Write commands automatically sync to replicas
- **PSYNC protocol**: Compatible with Redis replication protocol

### 📊 Advanced Data Types

- **Lists**: LPUSH, LPOP, RPUSH, BLPOP with blocking operations
- **Streams**: XADD, XREAD, XRANGE for event streaming
- **Expiration**: Automatic cleanup of expired keys

## Quick Start

### Build

```bash
git clone https://github.com/kennythai2003/reddish.git
cd reddish
make
```

### Run Server

```bash
# Basic server
./server

# With custom RDB file
./server --dir /path/to/data --dbfilename dump.rdb

# As replica
./server --replicaof localhost 6380
```

## Usage Examples

### Basic Operations

```bash
# Connect with redis-cli
redis-cli -p 6379

# Set and get values
SET mykey "hello world"
GET mykey

# Set with expiration (2 seconds)
SET tempkey "expires soon" PX 2000
GET tempkey  # returns value
# wait 2 seconds...
GET tempkey  # returns (nil)
```

### Lists

```bash
# Push to list
LPUSH mylist "item1" "item2" "item3"

# Pop from list (left side)
LPOP mylist

# Blocking pop (waits for items)
BLPOP mylist 5  # wait up to 5 seconds
```

### Streams

```bash
# Add to stream
XADD mystream * field1 value1 field2 value2

# Read from stream
XREAD STREAMS mystream 0

# Read specific range
XRANGE mystream - +
```

### Replication

```bash
# Start primary server
./server --port 6379

# Start replica server
./server --port 6380 --replicaof localhost 6379

# Commands on primary automatically sync to replica
redis-cli -p 6379 SET key1 value1
redis-cli -p 6380 GET key1  # returns value1
```

### Data Persistence

```bash
# Server loads RDB file on startup
./server --dir /data --dbfilename backup.rdb

# Keys from RDB file are immediately available
redis-cli GET persistent_key
```

## Configuration

| Option                      | Description        | Default           |
| --------------------------- | ------------------ | ----------------- |
| `--port`                    | Server port        | 6379              |
| `--dir`                     | RDB file directory | current directory |
| `--dbfilename`              | RDB filename       | dump.rdb          |
| `--replicaof <host> <port>` | Connect as replica | none              |

## Protocol Compatibility

Reddish implements the Redis Serialization Protocol (RESP) and is compatible with:

- `redis-cli`
- Redis client libraries (Python `redis`, Node.js `ioredis`, etc.)
- Redis GUI tools

## Architecture

```
src/
├── Server.cpp          # Main server and event loop
├── CommandHandler.cpp  # Redis command processing
├── RespParser.cpp      # RESP protocol parsing
├── RdbParser.cpp       # RDB file format handling
└── *.h                 # Header files
```

## Testing

```bash
# Build and run basic test
make
python3 build/test_simple.py

# Test expiry functionality
python3 build/test_comprehensive_expiry.py
```

## Implementation Notes

- **Single-threaded**: Uses epoll/kqueue for efficient I/O multiplexing
- **Memory efficient**: Automatic cleanup of expired keys
- **Redis compatible**: Passes Redis protocol compliance tests
- **Clean codebase**: Well-structured C++ with proper separation of concerns

---

_Built as part of the CodeCrafters Redis challenge - proving that understanding the fundamentals can recreate sophisticated systems._
