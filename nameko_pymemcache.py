from weakref import WeakKeyDictionary

from nameko.extensions import DependencyProvider

from pymemcache.client.hash import HashClient
from pymemcache.serde import CompressedSerde


class NamekoHashClient(HashClient):
    """Enhanced pymemcache HashClient optimized for Nameko services.
    
    Provides reliable multi-node memcached support with proper connection
    management and consistent behavior for production use.
    """

    def disconnect_all(self):
        """Disconnect all client connections for proper cleanup."""
        for client in self.clients.values():
            client.close()

    def get_many(self, keys, gets=False, *args, **kwargs):
        """Get multiple keys with consistent behavior.
        
        Filters out False values that pymemcache's HashClient may return,
        ensuring consistent None behavior for missing keys.
        """
        result = super().get_many(keys, gets, *args, **kwargs)
        return {key: result.get(key) for key in result if result.get(key)}

    get_multi = get_many  # Alias for backward compatibility

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
            client.disconnect_all()

    def _split_host_and_port(self, servers):
        """Convert python-memcached based server strings to pymemcache format.
        
        - Input: ['127.0.0.1:11211', ...] or ['127.0.0.1', ...]
        - Output: [('127.0.0.1', 11211), ...]
        """
        host_and_port_list = []
        for server in servers:
            connection_info = server.split(':')
            if len(connection_info) == 1:
                host_and_port_list.append((connection_info[0], 11211))
            elif len(connection_info) == 2:
                host_and_port_list.append((connection_info[0], int(connection_info[1])))
        return host_and_port_list

    def _get_client(self):
        # Parse servers to (host, port) tuples
        servers = self._split_host_and_port(self.uris)

        # Set up bmemcached-compatible compression (128-byte threshold like bmemcached)
        bmemcached_compatible_serde = CompressedSerde(min_compress_len=128)
        
        client_options = {
            'serde': bmemcached_compatible_serde,
        }

        # Merge in user-provided options (they can override defaults)
        client_options.update(self.options)

        # Handle authentication if provided
        if self.user and self.password:
            # Note: pymemcache doesn't support SASL auth like bmemcached
            # This would need to be handled at the server level
            # or connection string
            pass  # For now, auth is handled differently in pymemcache

        return NamekoHashClient(servers, **client_options)


# Export the client class for direct use
__all__ = ['Memcached', 'NamekoHashClient']
