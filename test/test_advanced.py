import eventlet
eventlet.monkey_patch()  # noqa (code before rest of imports)

from nameko.containers import ServiceContainer  # noqa
from nameko.testing.services import entrypoint_hook, dummy  # noqa

import pytest  # noqa
import time
from unittest.mock import patch, MagicMock

from pymemcache.client.base import Client  # noqa
from pymemcache.serde import (  # noqa
    python_memcache_serializer,
    python_memcache_deserializer
)

from nameko_pymemcache import Memcached, NamekoHashClient  # noqa


class AdvancedTestService(object):
    name = "advancedtestservice"
    memcached = Memcached()

    @dummy
    def set_value(self, key, value, expire=None):
        if expire:
            return self.memcached.set(key, value, expire=expire)
        return self.memcached.set(key, value)

    @dummy
    def get_value(self, key):
        return self.memcached.get(key)

    @dummy
    def delete_value(self, key):
        return self.memcached.delete(key)

    @dummy
    def set_many_values(self, mapping):
        return self.memcached.set_many(mapping)

    @dummy
    def get_many_values(self, keys):
        return self.memcached.get_many(keys)

    @dummy
    def increment_value(self, key, delta=1):
        return self.memcached.incr(key, delta)

    @dummy
    def decrement_value(self, key, delta=1):
        return self.memcached.decr(key, delta)


@pytest.fixture
def cleanup_keys():
    """Fixture to clean up test keys after each test."""
    keys_to_cleanup = []
    
    def add_key(key):
        keys_to_cleanup.append(key)
    
    yield add_key
    
    # Cleanup after test
    client = Client(
        ('127.0.0.1', 11211),
        serializer=python_memcache_serializer,
        deserializer=python_memcache_deserializer
    )
    
    for key in keys_to_cleanup:
        try:
            client.delete(key)
        except:
            pass  # Ignore cleanup errors
    
    client.quit()


@pytest.fixture
def test_container():
    """Fixture to provide a test container with memcached config."""
    config = {
        'MEMCACHED_URIS': ['127.0.0.1:11211']
    }
    
    container = ServiceContainer(AdvancedTestService, config)
    container.start()
    
    yield container
    
    container.stop()


class TestMultiNodeConfiguration:
    """Test multi-node memcached configuration and consistent hashing."""
    
    def test_single_node_configuration(self):
        """Test single node configuration works correctly."""
        hash_client = NamekoHashClient(
            [('127.0.0.1', 11211)],
            serializer=python_memcache_serializer,
            deserializer=python_memcache_deserializer
        )
        
        test_key = 'single_node_test'
        test_value = 'single_value'
        
        hash_client.set(test_key, test_value)
        result = hash_client.get(test_key)
        hash_client.delete(test_key)
        hash_client.disconnect_all()
        
        assert result == test_value

    def test_host_port_parsing(self):
        """Test that _split_host_and_port correctly parses server strings."""
        memcached = Memcached()
        
        # Test with port specified
        servers_with_port = ['127.0.0.1:11211', '192.168.1.10:11212']
        result = memcached._split_host_and_port(servers_with_port)
        expected = [('127.0.0.1', 11211), ('192.168.1.10', 11212)]
        assert result == expected
        
        # Test without port (should default to 11211)
        servers_no_port = ['127.0.0.1', '192.168.1.10']
        result = memcached._split_host_and_port(servers_no_port)
        expected = [('127.0.0.1', 11211), ('192.168.1.10', 11211)]
        assert result == expected
        
        # Test mixed
        servers_mixed = ['127.0.0.1:11211', '192.168.1.10']
        result = memcached._split_host_and_port(servers_mixed)
        expected = [('127.0.0.1', 11211), ('192.168.1.10', 11211)]
        assert result == expected

    def test_multi_node_config_setup(self):
        """Test that multi-node configuration is set up correctly."""
        config = {
            'MEMCACHED_URIS': ['127.0.0.1:11211', '127.0.0.1:11212', '127.0.0.1:11213']
        }
        
        memcached = Memcached()
        
        # Mock container
        mock_container = MagicMock()
        mock_container.config = config
        memcached.container = mock_container
        
        memcached.setup()
        
        assert memcached.uris == config['MEMCACHED_URIS']
        assert memcached.user is None
        assert memcached.password is None


class TestBatchOperations:
    """Test batch operations like get_many and set_many."""
    
    def test_set_many_get_many(self, test_container, cleanup_keys):
        """Test batch set and get operations."""
        test_data = {
            'batch_key1': 'value1',
            'batch_key2': 'value2',
            'batch_key3': 'value3'
        }
        
        # Add keys to cleanup
        for key in test_data.keys():
            cleanup_keys(key)
        
        # Set multiple values
        with entrypoint_hook(test_container, "set_many_values") as set_many:
            set_many(test_data)
        
        # Get multiple values
        with entrypoint_hook(test_container, "get_many_values") as get_many:
            result = get_many(list(test_data.keys()))
        
        # Verify all values were set and retrieved correctly
        for key, expected_value in test_data.items():
            assert key in result
            assert result[key] == expected_value

    def test_get_many_with_missing_keys(self, test_container, cleanup_keys):
        """Test get_many with some missing keys."""
        existing_data = {'existing_key1': 'value1', 'existing_key2': 'value2'}
        all_keys = ['existing_key1', 'existing_key2', 'missing_key1', 'missing_key2']
        
        # Add keys to cleanup
        for key in existing_data.keys():
            cleanup_keys(key)
        
        # Set some values
        with entrypoint_hook(test_container, "set_many_values") as set_many:
            set_many(existing_data)
        
        # Try to get all keys (including missing ones)
        with entrypoint_hook(test_container, "get_many_values") as get_many:
            result = get_many(all_keys)
        
        # Should only contain existing keys
        assert len(result) == 2
        assert 'existing_key1' in result
        assert 'existing_key2' in result
        assert 'missing_key1' not in result
        assert 'missing_key2' not in result

    def test_get_many_filters_false_values(self):
        """Test that get_many filters out False values as documented."""
        hash_client = NamekoHashClient(
            [('127.0.0.1', 11211)],
            serializer=python_memcache_serializer,
            deserializer=python_memcache_deserializer
        )
        
        try:
            # Set up test data including a False value
            test_keys = ['test_false1', 'test_false2', 'test_false3']
            hash_client.set('test_false1', 'real_value')
            hash_client.set('test_false2', False)  # This should be filtered out
            # test_false3 doesn't exist
            
            result = hash_client.get_many(test_keys)
            
            # Should only contain the real value, False and missing keys filtered out
            assert 'test_false1' in result
            assert result['test_false1'] == 'real_value'
            assert 'test_false2' not in result  # False value filtered
            assert 'test_false3' not in result  # Missing key
            
        finally:
            for key in test_keys:
                hash_client.delete(key)
            hash_client.disconnect_all()


class TestIncrementDecrement:
    """Test increment and decrement operations."""
    
    def test_increment_operations(self, test_container, cleanup_keys):
        """Test increment operations."""
        test_key = 'incr_test_key'
        cleanup_keys(test_key)
        
        # Set initial value
        with entrypoint_hook(test_container, "set_value") as set_val:
            set_val(test_key, 10)
        
        # Increment by 1 (default)
        with entrypoint_hook(test_container, "increment_value") as incr:
            result1 = incr(test_key)
        
        # Increment by 5
        with entrypoint_hook(test_container, "increment_value") as incr:
            result2 = incr(test_key, 5)
        
        # Get final value
        with entrypoint_hook(test_container, "get_value") as get_val:
            final_value = get_val(test_key)
        
        assert result1 == 11
        assert result2 == 16
        assert final_value == 16

    def test_decrement_operations(self, test_container, cleanup_keys):
        """Test decrement operations."""
        test_key = 'decr_test_key'
        cleanup_keys(test_key)
        
        # Set initial value
        with entrypoint_hook(test_container, "set_value") as set_val:
            set_val(test_key, 20)
        
        # Decrement by 1 (default)
        with entrypoint_hook(test_container, "decrement_value") as decr:
            result1 = decr(test_key)
        
        # Decrement by 3
        with entrypoint_hook(test_container, "decrement_value") as decr:
            result2 = decr(test_key, 3)
        
        # Get final value
        with entrypoint_hook(test_container, "get_value") as get_val:
            final_value = get_val(test_key)
        
        assert result1 == 19
        assert result2 == 16
        assert final_value == 16


class TestExpirationAndTTL:
    """Test expiration and TTL functionality."""
    
    def test_expiration_works(self, test_container, cleanup_keys):
        """Test that expiration works correctly."""
        test_key = 'expire_test_key'
        cleanup_keys(test_key)
        
        # Set value with 2 second expiration
        with entrypoint_hook(test_container, "set_value") as set_val:
            set_val(test_key, 'expire_value', expire=2)
        
        # Immediately check value exists
        with entrypoint_hook(test_container, "get_value") as get_val:
            result1 = get_val(test_key)
        
        assert result1 == 'expire_value'
        
        # Wait for expiration
        time.sleep(3)
        
        # Check value has expired
        with entrypoint_hook(test_container, "get_value") as get_val:
            result2 = get_val(test_key)
        
        assert result2 is None


class TestEdgeCases:
    """Test edge cases and special values."""
    
    def test_none_values(self, test_container, cleanup_keys):
        """Test handling of None values."""
        test_key = 'none_test_key'
        cleanup_keys(test_key)
        
        # Set None value
        with entrypoint_hook(test_container, "set_value") as set_val:
            set_val(test_key, None)
        
        # Get None value
        with entrypoint_hook(test_container, "get_value") as get_val:
            result = get_val(test_key)
        
        assert result is None

    def test_empty_string_values(self, test_container, cleanup_keys):
        """Test handling of empty string values."""
        test_key = 'empty_string_test'
        cleanup_keys(test_key)
        
        # Set empty string
        with entrypoint_hook(test_container, "set_value") as set_val:
            set_val(test_key, '')
        
        # Get empty string
        with entrypoint_hook(test_container, "get_value") as get_val:
            result = get_val(test_key)
        
        assert result == ''

    def test_complex_data_structures(self, test_container, cleanup_keys):
        """Test handling of complex Python data structures."""
        test_key = 'complex_data_test'
        cleanup_keys(test_key)
        
        complex_data = {
            'list': [1, 2, 3, 'string'],
            'dict': {'nested': {'key': 'value'}},
            'tuple': (1, 2, 'tuple'),
            'numbers': [1.5, 2.7, -3],
            'boolean': True
        }
        
        # Set complex data
        with entrypoint_hook(test_container, "set_value") as set_val:
            set_val(test_key, complex_data)
        
        # Get complex data
        with entrypoint_hook(test_container, "get_value") as get_val:
            result = get_val(test_key)
        
        assert result == complex_data

    def test_unicode_handling(self, test_container, cleanup_keys):
        """Test handling of unicode strings."""
        test_key = 'unicode_test'
        cleanup_keys(test_key)
        
        unicode_value = 'Hej! 🇸🇪 Här är svenska tecken: åäö'
        
        # Set unicode value
        with entrypoint_hook(test_container, "set_value") as set_val:
            set_val(test_key, unicode_value)
        
        # Get unicode value
        with entrypoint_hook(test_container, "get_value") as get_val:
            result = get_val(test_key)
        
        assert result == unicode_value


class TestConnectionManagement:
    """Test connection management and cleanup."""
    
    def test_disconnect_all_method(self):
        """Test that disconnect_all properly closes connections."""
        hash_client = NamekoHashClient(
            [('127.0.0.1', 11211)],
            serializer=python_memcache_serializer,
            deserializer=python_memcache_deserializer
        )
        
        # Use the client to establish connections
        hash_client.set('connection_test', 'test_value')
        
        # Mock the clients to verify disconnect_all calls close()
        mock_client = MagicMock()
        hash_client.clients = {'server1': mock_client}
        
        hash_client.disconnect_all()
        
        mock_client.close.assert_called_once()

    def test_worker_teardown_cleanup(self):
        """Test that worker teardown properly cleans up connections."""
        config = {
            'MEMCACHED_URIS': ['127.0.0.1:11211']
        }
        
        memcached = Memcached()
        
        # Mock container
        mock_container = MagicMock()
        mock_container.config = config
        memcached.container = mock_container
        
        memcached.setup()
        
        # Mock worker context
        mock_worker_ctx = MagicMock()
        
        # Get dependency (creates client)
        client = memcached.get_dependency(mock_worker_ctx)
        assert client is not None
        assert mock_worker_ctx in memcached.clients
        
        # Mock the disconnect_all method to verify it's called
        client.disconnect_all = MagicMock()
        
        # Teardown worker
        memcached.worker_teardown(mock_worker_ctx)
        
        # Verify client was removed and disconnect_all was called
        assert mock_worker_ctx not in memcached.clients
        client.disconnect_all.assert_called_once()


class TestConfigurationOptions:
    """Test various configuration options."""
    
    def test_custom_options_passed_to_client(self):
        """Test that custom options are passed to the pymemcache client."""
        custom_options = {
            'connect_timeout': 0.5,
            'timeout': 1.0,
            'retry_attempts': 3
        }
        
        config = {
            'MEMCACHED_URIS': ['127.0.0.1:11211']
        }
        
        memcached = Memcached(**custom_options)
        
        # Mock container
        mock_container = MagicMock()
        mock_container.config = config
        memcached.container = mock_container
        
        memcached.setup()
        
        # Mock NamekoHashClient to capture initialization arguments
        with patch('nameko_pymemcache.NamekoHashClient') as mock_hash_client:
            memcached._get_client()
            
            # Verify NamekoHashClient was called with custom options
            args, kwargs = mock_hash_client.call_args
            
            # Check that our custom options are in kwargs
            for key, value in custom_options.items():
                assert key in kwargs
                assert kwargs[key] == value

    def test_authentication_config(self):
        """Test authentication configuration setup."""
        config = {
            'MEMCACHED_URIS': ['127.0.0.1:11211'],
            'MEMCACHED_USER': 'testuser',
            'MEMCACHED_PASSWORD': 'testpass'
        }
        
        memcached = Memcached()
        
        # Mock container
        mock_container = MagicMock()
        mock_container.config = config
        memcached.container = mock_container
        
        memcached.setup()
        
        assert memcached.user == 'testuser'
        assert memcached.password == 'testpass'

    def test_default_serialization_options(self):
        """Test that default serialization options are set correctly."""
        config = {
            'MEMCACHED_URIS': ['127.0.0.1:11211']
        }
        
        memcached = Memcached()
        
        # Mock container
        mock_container = MagicMock()
        mock_container.config = config
        memcached.container = mock_container
        
        memcached.setup()
        
        # Mock NamekoHashClient to capture initialization arguments
        with patch('nameko_pymemcache.NamekoHashClient') as mock_hash_client:
            memcached._get_client()
            
            args, kwargs = mock_hash_client.call_args
            
            # Check that default serialization is set
            assert 'serializer' in kwargs
            assert 'deserializer' in kwargs
            assert kwargs['serializer'] == python_memcache_serializer
            assert kwargs['deserializer'] == python_memcache_deserializer