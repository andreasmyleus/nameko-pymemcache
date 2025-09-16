# nameko-pymemcache
[![PyPI version](https://badge.fury.io/py/nameko-pymemcache.svg)](https://badge.fury.io/py/nameko-pymemcache)
[![Tests](https://github.com/andreasmyleus/nameko-pymemcache/actions/workflows/test.yml/badge.svg)](https://github.com/andreasmyleus/nameko-pymemcache/actions/workflows/test.yml)
[![Python](https://img.shields.io/pypi/pyversions/nameko-pymemcache.svg)](https://pypi.org/project/nameko-pymemcache/)

Memcached dependency for nameko services with consistent hashing support for multi-node setups. Uses the high-performance pymemcache library with automatic failover and connection pooling.

**Key Features:**
- **Consistent hashing** for reliable multi-node memcached clusters  
- **Optimized for Nameko** - proper connection management and worker cleanup
- **Automatic failover** when nodes become unavailable
- **Drop-in replacement** for bmemcached with better multi-node behavior
- **High performance** - uses pymemcache's efficient C implementation

Inspiration and structure **proudly** stolen from nameko-redis :) Thanks guys!

## Installation
```
pip install nameko-pymemcache
```

## Usage
```python
from nameko.rpc import rpc
from nameko_pymemcache import Memcached


class MyService(object):
    name = "my_service"

    memcached = Memcached()

    @rpc
    def hello(self, name):
        self.memcached.set("foo", name)
        return "Hello, {}!".format(name)

    @rpc
    def bye(self):
        name = self.memcached.get("foo")
        return "Bye, {}!".format(name)
```

To specify memcached uri(s) and optional username/password you will need a config
```yaml
AMQP_URI: 'amqp://guest:guest@localhost'
MEMCACHED_URIS: ['127.0.0.1:11211', ]
MEMCACHED_USER: 'playerone'
MEMCACHED_PASSWORD: 'ready'
```

## Multi-Node Configuration

For multi-node memcached clusters, specify multiple servers using either format:

**YAML list format:**
```yaml
AMQP_URI: 'amqp://guest:guest@localhost'
MEMCACHED_URIS: 
  - '192.168.1.10:11211'
  - '192.168.1.11:11211'
  - '192.168.1.12:11211'
```

**Bracketed list format:**
```yaml
AMQP_URI: 'amqp://guest:guest@localhost'
MEMCACHED_URIS: ['192.168.1.10:11211', '192.168.1.11:11211', '192.168.1.12:11211']
```

The client automatically uses **consistent hashing** to distribute keys across nodes. When a node fails, only the keys on that node are affected (not all keys like with simple round-robin).

## Advanced Configuration

You can pass extra options to customize client behavior:
```python
class MyService(object):
    name = "my_service"

    memcached = Memcached(
        connect_timeout=0.1,    # connection timeout in seconds
        timeout=0.2,            # operation timeout in seconds
        retry_attempts=2,       # number of retries on failure
        dead_timeout=10,        # how long to avoid a failed node
    )

    ...
```

## Available Operations

All standard memcached operations are supported:

```python
# Basic operations
self.memcached.set(key, value, expire=300)
result = self.memcached.get(key)
self.memcached.delete(key)

# Batch operations
self.memcached.set_many({'key1': 'val1', 'key2': 'val2'})
results = self.memcached.get_many(['key1', 'key2'])

# Increment/decrement operations
self.memcached.incr(key, delta=1)
self.memcached.decr(key, delta=1)
```

## Performance Tips

- **Identical server order**: Keep the same server order across all clients for consistent key distribution
- **Connection pooling**: Available via pymemcache options if needed
- **Custom timeouts**: Override defaults by passing pymemcache options to the constructor
- **Failure handling**: Failed nodes are automatically removed from the hash ring and retried later
