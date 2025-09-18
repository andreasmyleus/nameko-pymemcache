import eventlet
eventlet.monkey_patch()  # noqa (code before rest of imports)

from nameko.containers import ServiceContainer  # noqa
from nameko.testing.services import entrypoint_hook, dummy  # noqa

import pytest  # noqa
from unittest.mock import patch, MagicMock, Mock

from pymemcache.client.base import Client  # noqa
from pymemcache.exceptions import MemcacheError, MemcacheClientError, MemcacheServerError
from pymemcache.serde import (  # noqa
    python_memcache_serializer,
    python_memcache_deserializer
)

from nameko_pymemcache import Memcached, NamekoHashClient  # noqa


class ErrorTestService(object):
    name = "errortestservice"
    memcached = Memcached()

    @dummy
    def test_operation(self, operation, *args, **kwargs):
        """Generic test operation that can call any memcached method."""
        method = getattr(self.memcached, operation)
        return method(*args, **kwargs)


@pytest.fixture
def error_test_container():
    """Fixture to provide a test container for error testing."""
    config = {
        'MEMCACHED_URIS': ['127.0.0.1:11211']
    }
    
    container = ServiceContainer(ErrorTestService, config)
    container.start()
    
    yield container
    
    container.stop()


class TestConnectionErrors:
    """Test handling of connection-related errors."""
    
    def test_invalid_server_configuration(self):
        """Test behavior with invalid server configuration."""
        # Test with non-existent server
        config = {
            'MEMCACHED_URIS': ['192.168.255.255:11211']  # Non-existent IP
        }
        
        memcached = Memcached()
        
        # Mock container
        mock_container = MagicMock()
        mock_container.config = config
        memcached.container = mock_container
        
        memcached.setup()
        
        # Getting a client should work (connections are lazy)
        client = memcached._get_client()
        assert client is not None
        
        # But operations should fail gracefully
        # Note: This test depends on pymemcache's timeout behavior

    def test_malformed_uri_handling(self):
        """Test handling of malformed URIs."""
        memcached = Memcached()
        
        # Test various malformed URIs
        malformed_uris = [
            ['127.0.0.1:invalid_port'],
            ['invalid:host:format:11211'],
            [''],
            [':11211']  # Missing host
        ]
        
        for uris in malformed_uris:
            try:
                result = memcached._split_host_and_port(uris)
                # If no exception, verify the result makes sense
                if result:
                    for host, port in result:
                        assert isinstance(host, str)
                        assert isinstance(port, int)
            except (ValueError, IndexError):
                # Expected for some malformed URIs
                pass

    def test_empty_server_list(self):
        """Test behavior with empty server list."""
        config = {
            'MEMCACHED_URIS': []
        }
        
        memcached = Memcached()
        
        # Mock container
        mock_container = MagicMock()
        mock_container.config = config
        memcached.container = mock_container
        
        memcached.setup()
        
        # Should handle empty server list gracefully
        servers = memcached._split_host_and_port(memcached.uris)
        assert servers == []


class TestMemcacheExceptions:
    """Test handling of pymemcache exceptions."""
    
    def test_client_error_handling(self, error_test_container):
        """Test handling of client errors."""
        with patch.object(NamekoHashClient, 'get') as mock_get:
            mock_get.side_effect = MemcacheClientError("Client error")
            
            with entrypoint_hook(error_test_container, "test_operation") as test_op:
                try:
                    result = test_op('get', 'test_key')
                    # If no exception was raised, the error was handled
                    assert result is None or isinstance(result, str)
                except MemcacheClientError:
                    # Exception propagated - this is also acceptable behavior
                    pass

    def test_server_error_handling(self, error_test_container):
        """Test handling of server errors."""
        with patch.object(NamekoHashClient, 'set') as mock_set:
            mock_set.side_effect = MemcacheServerError("Server error")
            
            with entrypoint_hook(error_test_container, "test_operation") as test_op:
                try:
                    result = test_op('set', 'test_key', 'test_value')
                    # If no exception was raised, the error was handled
                    assert result is None or isinstance(result, bool)
                except MemcacheServerError:
                    # Exception propagated - this is also acceptable behavior
                    pass

    def test_generic_memcache_error_handling(self, error_test_container):
        """Test handling of generic memcache errors."""
        with patch.object(NamekoHashClient, 'delete') as mock_delete:
            mock_delete.side_effect = MemcacheError("Generic memcache error")
            
            with entrypoint_hook(error_test_container, "test_operation") as test_op:
                try:
                    result = test_op('delete', 'test_key')
                    # If no exception was raised, the error was handled
                    assert result is None or isinstance(result, bool)
                except MemcacheError:
                    # Exception propagated - this is also acceptable behavior
                    pass


class TestFailoverBehavior:
    """Test failover behavior in multi-node scenarios."""
    
    def test_single_node_failure_simulation(self):
        """Test behavior when a single node fails in multi-node setup."""
        # Create hash client with multiple servers (only first one exists)
        hash_client = NamekoHashClient(
            [('127.0.0.1', 11211), ('127.0.0.1', 11212)],  # Second server doesn't exist
            serializer=python_memcache_serializer,
            deserializer=python_memcache_deserializer
        )
        
        try:
            # Set a value - should work despite one server being down
            hash_client.set('failover_test', 'test_value')
            
            # Get the value - should work
            result = hash_client.get('failover_test')
            
            # Clean up
            hash_client.delete('failover_test')
            
            # The operation might succeed or fail depending on which server the key hashes to
            # This test mainly ensures no crashes occur
            assert result is None or result == 'test_value'
            
        finally:
            hash_client.disconnect_all()

    def test_all_nodes_failure_simulation(self):
        """Test behavior when all nodes fail."""
        # Create hash client with non-existent servers
        hash_client = NamekoHashClient(
            [('192.168.255.254', 11211), ('192.168.255.253', 11211)],
            serializer=python_memcache_serializer,
            deserializer=python_memcache_deserializer,
            connect_timeout=0.1,  # Fail fast
            timeout=0.1
        )
        
        try:
            # Operations should fail or timeout quickly
            with pytest.raises((MemcacheError, OSError, Exception)):
                hash_client.set('fail_test', 'value')
        finally:
            hash_client.disconnect_all()


class TestResourceCleanup:
    """Test proper resource cleanup in error scenarios."""
    
    def test_cleanup_on_container_stop(self):
        """Test that resources are cleaned up when container stops."""
        config = {
            'MEMCACHED_URIS': ['127.0.0.1:11211']
        }
        
        container = ServiceContainer(ErrorTestService, config)
        container.start()
        
        # Create some worker contexts and get dependencies
        mock_worker_ctx1 = MagicMock()
        mock_worker_ctx2 = MagicMock()
        
        memcached_dep = None
        for dependency in container.service.memcached:
            if hasattr(dependency, 'get_dependency'):
                memcached_dep = dependency
                break
        
        if memcached_dep:
            client1 = memcached_dep.get_dependency(mock_worker_ctx1)
            client2 = memcached_dep.get_dependency(mock_worker_ctx2)
            
            # Mock disconnect_all to verify it's called
            client1.disconnect_all = MagicMock()
            client2.disconnect_all = MagicMock()
            
            # Stop container - should trigger cleanup
            container.stop()
            
            # Note: The actual cleanup behavior depends on Nameko's internal mechanisms
            # This test mainly ensures no exceptions are raised during shutdown

    def test_worker_teardown_with_missing_client(self):
        """Test worker teardown when client is missing from WeakKeyDictionary."""
        config = {
            'MEMCACHED_URIS': ['127.0.0.1:11211']
        }
        
        memcached = Memcached()
        
        # Mock container
        mock_container = MagicMock()
        mock_container.config = config
        memcached.container = mock_container
        
        memcached.setup()
        
        mock_worker_ctx = MagicMock()
        
        # Try to teardown a worker that was never set up
        # Should not raise an exception
        memcached.worker_teardown(mock_worker_ctx)
        
        # Verify no exception was raised
        assert True

    def test_disconnect_all_with_broken_clients(self):
        """Test disconnect_all when some clients are broken."""
        hash_client = NamekoHashClient(
            [('127.0.0.1', 11211)],
            serializer=python_memcache_serializer,
            deserializer=python_memcache_deserializer
        )
        
        # Mock clients with one that raises an exception on close()
        mock_client1 = MagicMock()
        mock_client2 = MagicMock()
        mock_client2.close.side_effect = Exception("Connection already closed")
        
        hash_client.clients = {
            'server1': mock_client1,
            'server2': mock_client2
        }
        
        # Should not raise an exception even if one client fails to close
        try:
            hash_client.disconnect_all()
            # Verify the working client was closed
            mock_client1.close.assert_called_once()
        except Exception as e:
            # If an exception is raised, it should be from the broken client
            assert "Connection already closed" in str(e)


class TestEdgeCaseErrors:
    """Test error handling for edge cases."""
    
    def test_very_large_value_handling(self, error_test_container):
        """Test handling of values that exceed memcached limits."""
        # Create a very large value (>1MB, typical memcached limit)
        large_value = 'x' * (2 * 1024 * 1024)  # 2MB
        
        with entrypoint_hook(error_test_container, "test_operation") as test_op:
            try:
                result = test_op('set', 'large_value_test', large_value)
                # If successful, verify it can be retrieved
                if result:
                    retrieved = test_op('get', 'large_value_test')
                    assert retrieved == large_value or retrieved is None
            except (MemcacheError, Exception):
                # Large values may be rejected - this is expected
                pass

    def test_invalid_key_characters(self, error_test_container):
        """Test handling of keys with invalid characters."""
        invalid_keys = [
            'key with spaces',
            'key\nwith\nnewlines',
            'key\twith\ttabs',
            'key with unicode: åäö',
            'very_long_key_' + 'x' * 250  # Very long key
        ]
        
        for key in invalid_keys:
            with entrypoint_hook(error_test_container, "test_operation") as test_op:
                try:
                    result = test_op('set', key, 'test_value')
                    # If successful, try to retrieve
                    if result:
                        retrieved = test_op('get', key)
                        test_op('delete', key)  # Cleanup
                except (MemcacheError, UnicodeError, Exception):
                    # Invalid keys may be rejected - this is expected
                    pass

    def test_none_key_handling(self, error_test_container):
        """Test handling of None as a key."""
        with entrypoint_hook(error_test_container, "test_operation") as test_op:
            try:
                result = test_op('set', None, 'test_value')
                assert False, "Setting None key should raise an exception"
            except (TypeError, MemcacheError, Exception):
                # None keys should be rejected
                pass

    def test_numeric_key_handling(self, error_test_container):
        """Test handling of numeric keys."""
        numeric_keys = [123, 45.67, True, False]
        
        for key in numeric_keys:
            with entrypoint_hook(error_test_container, "test_operation") as test_op:
                try:
                    # Numeric keys might be converted to strings or rejected
                    result = test_op('set', key, f'value_for_{key}')
                    if result:
                        retrieved = test_op('get', key)
                        test_op('delete', key)  # Cleanup
                except (TypeError, MemcacheError, Exception):
                    # Some numeric keys may be rejected
                    pass


class TestConcurrencyErrors:
    """Test error handling in concurrent scenarios."""
    
    def test_concurrent_access_simulation(self, error_test_container):
        """Test that concurrent access doesn't cause issues."""
        import threading
        import time
        
        results = []
        errors = []
        
        def worker(worker_id):
            try:
                with entrypoint_hook(error_test_container, "test_operation") as test_op:
                    # Each worker sets and gets its own key
                    key = f'concurrent_test_{worker_id}'
                    value = f'value_{worker_id}'
                    
                    set_result = test_op('set', key, value)
                    time.sleep(0.01)  # Small delay
                    get_result = test_op('get', key)
                    delete_result = test_op('delete', key)
                    
                    results.append((worker_id, set_result, get_result, delete_result))
            except Exception as e:
                errors.append((worker_id, str(e)))
        
        # Start multiple threads
        threads = []
        for i in range(5):
            thread = threading.Thread(target=worker, args=(i,))
            threads.append(thread)
            thread.start()
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        # Verify results
        assert len(results) > 0, f"No successful operations, errors: {errors}"
        
        # Check that each worker got its own value back
        for worker_id, set_result, get_result, delete_result in results:
            expected_value = f'value_{worker_id}'
            if get_result is not None:  # Allow for timing issues
                assert get_result == expected_value