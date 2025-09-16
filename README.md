# nameko-pymemcache
[![PyPI version](https://badge.fury.io/py/nameko-pymemcache.svg)](https://badge.fury.io/py/nameko-pymemcache)
[![Build Status](https://travis-ci.org/andreasmyleus/nameko-pymemcache.svg?branch=master)](https://travis-ci.org/andreasmyleus/nameko-pymemcache)

Memcached dependency for nameko services with consistent hashing support for multi-node setups. Uses the high-performance pymemcache library with automatic failover and connection pooling.

**Key Features:**
- **Consistent hashing** for reliable multi-node memcached clusters
- **Automatic failover** when nodes become unavailable
- **Fast timeouts** to prevent application blocking on cache failures
- **Connection pooling** for optimal performance
- **Django-style sharding** compatible with existing Django cache configurations

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

For multi-node memcached clusters, simply specify multiple servers:
```yaml
AMQP_URI: 'amqp://guest:guest@localhost'
MEMCACHED_URIS: 
  - '192.168.1.10:11211'
  - '192.168.1.11:11211'
  - '192.168.1.12:11211'
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

## Performance Tips

- **Identical server order**: Keep the same server order across all clients for consistent key distribution
- **Fast timeouts**: The default 50ms timeouts prevent cache issues from blocking your application
- **Connection pooling**: Enabled by default for better performance under load
- **Failure handling**: Failed nodes are automatically removed from the ring and retried later
