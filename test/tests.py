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

    client = Client(
        ('127.0.0.1', 11211),
        serializer=python_memcache_serializer,
        deserializer=python_memcache_deserializer
    )
    client.delete(TEST_KEY)
    client.quit()


def test_end_to_end(memcached):
    config = {
        'MEMCACHED_URIS': [memcached, ]
    }

    container = ServiceContainer(ExampleService, config)
    container.start()

    try:
        # write through the service
        with entrypoint_hook(container, "write") as write:
            write("foobar")

        # verify changes written to memcached
        client = Client(
            ('127.0.0.1', 11211),
            serializer=python_memcache_serializer,
            deserializer=python_memcache_deserializer
        )
        assert client.get(TEST_KEY) == "foobar"
        client.quit()

        # read through the service
        with entrypoint_hook(container, "read") as read:
            assert read() == "foobar"
    finally:
        container.stop()
