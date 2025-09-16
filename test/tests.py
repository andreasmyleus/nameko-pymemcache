import eventlet
eventlet.monkey_patch()  # noqa (code before rest of imports)

from nameko.containers import ServiceContainer  # noqa
from nameko.testing.services import entrypoint_hook, dummy  # noqa

import pytest  # noqa

from pymemcache.client.base import Client  # noqa
from pymemcache.serde import (  # noqa
    python_memcache_serializer,
    python_memcache_deserializer
)

from nameko_pymemcache import Memcached  # noqa


TEST_KEY = 'nameko-test-value'


class ExampleService(object):
    name = "exampleservice"
    memcached = Memcached()

    @dummy
    def write(self, value):
        self.memcached.set(TEST_KEY, value)

    @dummy
    def read(self):
        return self.memcached.get(TEST_KEY)


@pytest.fixture
def memcached():
    uri = '127.0.0.1:11211'

    yield uri

    # Cleanup with both client types to be safe
    client = Client(
        ('127.0.0.1', 11211),
        serializer=python_memcache_serializer,
        deserializer=python_memcache_deserializer
    )
    client.delete(TEST_KEY)
    client.quit()

    # Also cleanup with our Nameko hash client
    from nameko_pymemcache import NamekoHashClient
    hash_client = NamekoHashClient(
        [('127.0.0.1', 11211)],
        serializer=python_memcache_serializer,
        deserializer=python_memcache_deserializer
    )
    hash_client.delete(TEST_KEY)
    hash_client.disconnect_all()


def test_memcached_connection():
    """Test that memcached is accessible and working."""
    client = Client(
        ('127.0.0.1', 11211),
        serializer=python_memcache_serializer,
        deserializer=python_memcache_deserializer
    )
    
    # Test basic connectivity
    test_key = 'connectivity_test'
    test_value = 'test_value'
    
    client.set(test_key, test_value)
    result = client.get(test_key)
    client.delete(test_key)
    client.quit()
    
    assert result == test_value, f"Basic memcached test failed: got {result}, expected {test_value}"


def test_hash_client_direct():
    """Test that our NamekoHashClient works directly."""
    from nameko_pymemcache import NamekoHashClient
    hash_client = NamekoHashClient(
        [('127.0.0.1', 11211)],
        serializer=python_memcache_serializer,
        deserializer=python_memcache_deserializer
    )
    
    test_key = 'hash_test'
    test_value = 'hash_value'
    
    hash_client.set(test_key, test_value)
    result = hash_client.get(test_key)
    hash_client.delete(test_key)
    hash_client.disconnect_all()
    
    assert result == test_value, f"HashClient test failed: got {result}, expected {test_value}"


def test_end_to_end(memcached):
    config = {
        'MEMCACHED_URIS': [memcached, ]
    }

    container = ServiceContainer(ExampleService, config)
    container.start()

    try:
        # write through the service
        with entrypoint_hook(container, "write") as write:
            result = write("foobar")
            print(f"Write result: {result}")

        # verify changes written to memcached directly
        client = Client(
            ('127.0.0.1', 11211),
            serializer=python_memcache_serializer,
            deserializer=python_memcache_deserializer
        )
        
        # Debug: check if key exists
        value = client.get(TEST_KEY)
        print(f"Direct memcached get result: {value}")
        print(f"Expected: 'foobar'")
        
        # Test reading through the service first (should work if write worked)
        with entrypoint_hook(container, "read") as read:
            service_value = read()
            print(f"Service read result: {service_value}")

        # Both should return the same value
        assert service_value == "foobar", f"Service read failed: got {service_value}, expected 'foobar'"
        assert value == "foobar", f"Direct memcached read failed: got {value}, expected 'foobar'"
        
        client.quit()
    finally:
        container.stop()
