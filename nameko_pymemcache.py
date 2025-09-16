from weakref import WeakKeyDictionary

from nameko.extensions import DependencyProvider

from pymemcache.client.hash import HashClient
from pymemcache.serde import (
    python_memcache_serializer,
    python_memcache_deserializer
)

# Version is handled automatically by setuptools_scm
try:
    from importlib.metadata import version
    __version__ = version("nameko-pymemcache")
except ImportError:
    # Fallback for older Python versions or development mode
    try:
        import pkg_resources
        __version__ = pkg_resources.get_distribution("nameko-pymemcache").version
    except Exception:
        __version__ = "0.0.0.dev0"


class Memcached(DependencyProvider):
    def __init__(self, **options):
        self.clients = WeakKeyDictionary()
        self.options = options

    def setup(self):
        self.uris = self.container.config['MEMCACHED_URIS']
        self.user = self.container.config.get('MEMCACHED_USER', None)
        self.password = self.container.config.get('MEMCACHED_PASSWORD', None)

    def get_dependency(self, worker_ctx):
        client = self._get_client()
        self.clients[worker_ctx] = client
        return client

    def worker_teardown(self, worker_ctx):
        client = self.clients.pop(worker_ctx, None)
        if client:
            client.quit()

    def _get_client(self):
        # Parse servers to (host, port) tuples
        servers = []
        for uri in self.uris:
            if ':' in uri:
                host, port = uri.rsplit(':', 1)
                port = int(port)
            else:
                host = uri
                port = 11211
            servers.append((host, port))

        # Set up default options for consistent hashing and reliability
        client_options = {
            'serializer': python_memcache_serializer,
            'deserializer': python_memcache_deserializer,
            'connect_timeout': 0.05,  # fail fast on connection
            'timeout': 0.05,          # fail fast on operations
            'no_delay': True,         # TCP_NODELAY for low latency
            'ignore_exc': True,       # never crash app on cache errors
            'retry_attempts': 1,      # minimal retry
            'dead_timeout': 5,        # temporarily evict sick nodes
            'use_pooling': True,      # connection pooling
        }

        # Merge in user-provided options (they can override defaults)
        client_options.update(self.options)

        # Handle authentication if provided
        if self.user and self.password:
            # Note: pymemcache doesn't support SASL auth like bmemcached
            # This would need to be handled at the server level
            # or connection string
            pass  # For now, auth is handled differently in pymemcache

        return HashClient(servers, **client_options)
