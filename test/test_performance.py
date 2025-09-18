import eventlet
eventlet.monkey_patch()  # noqa (code before rest of imports)

from nameko.containers import ServiceContainer  # noqa
from nameko.testing.services import entrypoint_hook, dummy  # noqa

import pytest  # noqa
import time
import statistics
from concurrent.futures import ThreadPoolExecutor, as_completed

from pymemcache.client.base import Client  # noqa
from pymemcache.serde import (  # noqa
    python_memcache_serializer,
    python_memcache_deserializer
)

from nameko_pymemcache import Memcached, NamekoHashClient  # noqa


class PerformanceTestService(object):
    name = "performancetestservice"
    memcached = Memcached()

    @dummy
    def set_value(self, key, value):
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


@pytest.fixture
def perf_test_container():
    """Fixture to provide a test container for performance testing."""
    config = {
        'MEMCACHED_URIS': ['127.0.0.1:11211']
    }
    
    container = ServiceContainer(PerformanceTestService, config)
    container.start()
    
    yield container
    
    container.stop()


@pytest.fixture
def cleanup_perf_keys():
    """Fixture to clean up performance test keys."""
    keys_to_cleanup = []
    
    def add_key(key):
        keys_to_cleanup.append(key)
    
    def add_keys(keys):
        keys_to_cleanup.extend(keys)
    
    yield add_key, add_keys
    
    # Cleanup after test
    if keys_to_cleanup:
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


class TestBasicPerformance:
    """Test basic performance characteristics."""
    
    def test_single_operation_latency(self, perf_test_container, cleanup_perf_keys):
        """Test latency of single operations."""
        add_key, _ = cleanup_perf_keys
        test_key = 'latency_test'
        add_key(test_key)
        
        # Measure set operation
        start_time = time.time()
        with entrypoint_hook(perf_test_container, "set_value") as set_val:
            set_val(test_key, 'test_value')
        set_time = time.time() - start_time
        
        # Measure get operation
        start_time = time.time()
        with entrypoint_hook(perf_test_container, "get_value") as get_val:
            result = get_val(test_key)
        get_time = time.time() - start_time
        
        # Basic assertions - operations should complete quickly
        assert set_time < 1.0, f"Set operation took {set_time:.3f}s, expected < 1.0s"
        assert get_time < 1.0, f"Get operation took {get_time:.3f}s, expected < 1.0s"
        assert result == 'test_value'
        
        print(f"Set latency: {set_time*1000:.2f}ms, Get latency: {get_time*1000:.2f}ms")

    def test_batch_operation_performance(self, perf_test_container, cleanup_perf_keys):
        """Test performance of batch operations vs individual operations."""
        _, add_keys = cleanup_perf_keys
        
        # Create test data
        num_items = 50
        test_data = {f'batch_perf_{i}': f'value_{i}' for i in range(num_items)}
        keys = list(test_data.keys())
        add_keys(keys)
        
        # Test batch set performance
        start_time = time.time()
        with entrypoint_hook(perf_test_container, "set_many_values") as set_many:
            set_many(test_data)
        batch_set_time = time.time() - start_time
        
        # Test individual set performance (for comparison)
        individual_keys = [f'individual_perf_{i}' for i in range(10)]  # Smaller sample
        add_keys(individual_keys)
        
        start_time = time.time()
        with entrypoint_hook(perf_test_container, "set_value") as set_val:
            for i, key in enumerate(individual_keys):
                set_val(key, f'value_{i}')
        individual_set_time = time.time() - start_time
        
        # Test batch get performance
        start_time = time.time()
        with entrypoint_hook(perf_test_container, "get_many_values") as get_many:
            batch_result = get_many(keys)
        batch_get_time = time.time() - start_time
        
        # Test individual get performance
        start_time = time.time()
        with entrypoint_hook(perf_test_container, "get_value") as get_val:
            individual_results = []
            for key in individual_keys:
                individual_results.append(get_val(key))
        individual_get_time = time.time() - start_time
        
        # Batch operations should be more efficient per item
        batch_set_per_item = batch_set_time / num_items
        individual_set_per_item = individual_set_time / len(individual_keys)
        
        batch_get_per_item = batch_get_time / num_items
        individual_get_per_item = individual_get_time / len(individual_keys)
        
        print(f"Batch set: {batch_set_per_item*1000:.2f}ms/item vs Individual: {individual_set_per_item*1000:.2f}ms/item")
        print(f"Batch get: {batch_get_per_item*1000:.2f}ms/item vs Individual: {individual_get_per_item*1000:.2f}ms/item")
        
        # Verify correctness
        assert len(batch_result) == num_items
        assert len(individual_results) == len(individual_keys)


class TestThroughputPerformance:
    """Test throughput under various conditions."""
    
    def test_sequential_throughput(self, perf_test_container, cleanup_perf_keys):
        """Test sequential operation throughput."""
        add_key, _ = cleanup_perf_keys
        
        num_operations = 100
        keys = [f'seq_throughput_{i}' for i in range(num_operations)]
        
        for key in keys:
            add_key(key)
        
        # Test sequential set throughput
        start_time = time.time()
        with entrypoint_hook(perf_test_container, "set_value") as set_val:
            for i, key in enumerate(keys):
                set_val(key, f'value_{i}')
        set_duration = time.time() - start_time
        
        # Test sequential get throughput
        start_time = time.time()
        with entrypoint_hook(perf_test_container, "get_value") as get_val:
            for key in keys:
                get_val(key)
        get_duration = time.time() - start_time
        
        set_throughput = num_operations / set_duration
        get_throughput = num_operations / get_duration
        
        print(f"Sequential SET throughput: {set_throughput:.1f} ops/sec")
        print(f"Sequential GET throughput: {get_throughput:.1f} ops/sec")
        
        # Basic throughput expectations
        assert set_throughput > 10, f"Set throughput {set_throughput:.1f} ops/sec too low"
        assert get_throughput > 10, f"Get throughput {get_throughput:.1f} ops/sec too low"

    @pytest.mark.slow
    def test_concurrent_throughput(self, perf_test_container, cleanup_perf_keys):
        """Test concurrent operation throughput."""
        add_key, _ = cleanup_perf_keys
        
        num_threads = 5
        ops_per_thread = 20
        
        def worker(thread_id):
            """Worker function for concurrent testing."""
            results = []
            keys = [f'concurrent_throughput_{thread_id}_{i}' for i in range(ops_per_thread)]
            
            # Add keys for cleanup
            for key in keys:
                add_key(key)
            
            start_time = time.time()
            
            with entrypoint_hook(perf_test_container, "set_value") as set_val:
                with entrypoint_hook(perf_test_container, "get_value") as get_val:
                    for i, key in enumerate(keys):
                        # Set and immediately get
                        set_val(key, f'value_{thread_id}_{i}')
                        result = get_val(key)
                        results.append(result)
            
            duration = time.time() - start_time
            return thread_id, results, duration
        
        # Run concurrent workers
        start_time = time.time()
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(worker, i) for i in range(num_threads)]
            results = [future.result() for future in as_completed(futures)]
        total_duration = time.time() - start_time
        
        # Analyze results
        total_operations = num_threads * ops_per_thread * 2  # set + get
        overall_throughput = total_operations / total_duration
        
        successful_operations = 0
        for thread_id, thread_results, thread_duration in results:
            successful_operations += len([r for r in thread_results if r is not None])
        
        print(f"Concurrent throughput: {overall_throughput:.1f} ops/sec")
        print(f"Successful operations: {successful_operations}/{num_threads * ops_per_thread}")
        
        # Verify most operations succeeded
        success_rate = successful_operations / (num_threads * ops_per_thread)
        assert success_rate > 0.8, f"Success rate {success_rate:.2f} too low"


class TestMemoryUsagePatterns:
    """Test memory usage patterns and efficiency."""
    
    def test_large_value_performance(self, perf_test_container, cleanup_perf_keys):
        """Test performance with different value sizes."""
        add_key, _ = cleanup_perf_keys
        
        value_sizes = [100, 1000, 10000, 100000]  # bytes
        results = {}
        
        for size in value_sizes:
            test_key = f'large_value_test_{size}'
            add_key(test_key)
            
            # Create value of specified size
            test_value = 'x' * size
            
            # Measure set time
            start_time = time.time()
            with entrypoint_hook(perf_test_container, "set_value") as set_val:
                set_val(test_key, test_value)
            set_time = time.time() - start_time
            
            # Measure get time
            start_time = time.time()
            with entrypoint_hook(perf_test_container, "get_value") as get_val:
                retrieved_value = get_val(test_key)
            get_time = time.time() - start_time
            
            results[size] = {
                'set_time': set_time,
                'get_time': get_time,
                'correct': retrieved_value == test_value
            }
            
            print(f"Size {size:6d} bytes: SET {set_time*1000:5.1f}ms, GET {get_time*1000:5.1f}ms")
        
        # Verify all operations succeeded
        for size, result in results.items():
            assert result['correct'], f"Value retrieval failed for size {size}"
            assert result['set_time'] < 5.0, f"Set time {result['set_time']:.2f}s too slow for size {size}"
            assert result['get_time'] < 5.0, f"Get time {result['get_time']:.2f}s too slow for size {size}"

    def test_key_distribution_performance(self, perf_test_container, cleanup_perf_keys):
        """Test performance with different key patterns."""
        add_key, _ = cleanup_perf_keys
        
        key_patterns = [
            ('short', lambda i: f'k{i}'),
            ('medium', lambda i: f'medium_key_{i}'),
            ('long', lambda i: f'very_long_key_name_with_lots_of_characters_{i}'),
            ('numeric', lambda i: str(i)),
            ('uuid_like', lambda i: f'{i:08x}-{i:04x}-{i:04x}-{i:04x}-{i:012x}')
        ]
        
        num_keys = 50
        
        for pattern_name, key_generator in key_patterns:
            keys = [key_generator(i) for i in range(num_keys)]
            
            for key in keys:
                add_key(key)
            
            # Test set performance
            start_time = time.time()
            with entrypoint_hook(perf_test_container, "set_value") as set_val:
                for i, key in enumerate(keys):
                    set_val(key, f'value_{i}')
            set_time = time.time() - start_time
            
            # Test get performance
            start_time = time.time()
            with entrypoint_hook(perf_test_container, "get_value") as get_val:
                for key in keys:
                    get_val(key)
            get_time = time.time() - start_time
            
            set_throughput = num_keys / set_time
            get_throughput = num_keys / get_time
            
            print(f"Pattern '{pattern_name}': SET {set_throughput:.1f} ops/sec, GET {get_throughput:.1f} ops/sec")
            
            # All patterns should achieve reasonable throughput
            assert set_throughput > 5, f"Set throughput too low for pattern {pattern_name}"
            assert get_throughput > 5, f"Get throughput too low for pattern {pattern_name}"


class TestClientOverhead:
    """Test overhead of the Nameko wrapper vs direct pymemcache usage."""
    
    def test_direct_vs_nameko_performance(self, cleanup_perf_keys):
        """Compare direct pymemcache client vs Nameko wrapper performance."""
        add_key, _ = cleanup_perf_keys
        
        num_operations = 50
        
        # Test direct pymemcache client
        direct_client = Client(
            ('127.0.0.1', 11211),
            serializer=python_memcache_serializer,
            deserializer=python_memcache_deserializer
        )
        
        direct_keys = [f'direct_perf_{i}' for i in range(num_operations)]
        for key in direct_keys:
            add_key(key)
        
        start_time = time.time()
        for i, key in enumerate(direct_keys):
            direct_client.set(key, f'direct_value_{i}')
            direct_client.get(key)
        direct_time = time.time() - start_time
        direct_client.quit()
        
        # Test NamekoHashClient
        hash_client = NamekoHashClient(
            [('127.0.0.1', 11211)],
            serializer=python_memcache_serializer,
            deserializer=python_memcache_deserializer
        )
        
        hash_keys = [f'hash_perf_{i}' for i in range(num_operations)]
        for key in hash_keys:
            add_key(key)
        
        start_time = time.time()
        for i, key in enumerate(hash_keys):
            hash_client.set(key, f'hash_value_{i}')
            hash_client.get(key)
        hash_time = time.time() - start_time
        hash_client.disconnect_all()
        
        # Calculate overhead
        overhead_ratio = hash_time / direct_time if direct_time > 0 else float('inf')
        
        print(f"Direct client: {direct_time:.3f}s ({num_operations*2/direct_time:.1f} ops/sec)")
        print(f"Hash client: {hash_time:.3f}s ({num_operations*2/hash_time:.1f} ops/sec)")
        print(f"Overhead ratio: {overhead_ratio:.2f}x")
        
        # Hash client should not be significantly slower
        assert overhead_ratio < 5.0, f"Hash client overhead {overhead_ratio:.2f}x too high"

    def test_connection_reuse_performance(self):
        """Test that connection reuse provides performance benefits."""
        num_operations = 20
        
        # Test with connection reuse (single client)
        reuse_client = NamekoHashClient(
            [('127.0.0.1', 11211)],
            serializer=python_memcache_serializer,
            deserializer=python_memcache_deserializer
        )
        
        start_time = time.time()
        for i in range(num_operations):
            key = f'reuse_perf_{i}'
            reuse_client.set(key, f'value_{i}')
            reuse_client.get(key)
            reuse_client.delete(key)
        reuse_time = time.time() - start_time
        reuse_client.disconnect_all()
        
        # Test without connection reuse (new client each time)
        start_time = time.time()
        for i in range(num_operations):
            new_client = NamekoHashClient(
                [('127.0.0.1', 11211)],
                serializer=python_memcache_serializer,
                deserializer=python_memcache_deserializer
            )
            key = f'new_perf_{i}'
            new_client.set(key, f'value_{i}')
            new_client.get(key)
            new_client.delete(key)
            new_client.disconnect_all()
        new_client_time = time.time() - start_time
        
        improvement_ratio = new_client_time / reuse_time if reuse_time > 0 else float('inf')
        
        print(f"Connection reuse: {reuse_time:.3f}s")
        print(f"New connections: {new_client_time:.3f}s")
        print(f"Improvement ratio: {improvement_ratio:.2f}x")
        
        # Connection reuse should be faster
        assert improvement_ratio > 1.0, f"Connection reuse should be faster, got {improvement_ratio:.2f}x"


@pytest.mark.slow
class TestStressTests:
    """Stress tests for high load scenarios."""
    
    def test_sustained_load(self, perf_test_container, cleanup_perf_keys):
        """Test performance under sustained load."""
        add_key, _ = cleanup_perf_keys
        
        duration_seconds = 10
        operations_count = 0
        errors = 0
        
        start_time = time.time()
        end_time = start_time + duration_seconds
        
        with entrypoint_hook(perf_test_container, "set_value") as set_val:
            with entrypoint_hook(perf_test_container, "get_value") as get_val:
                while time.time() < end_time:
                    try:
                        key = f'sustained_load_{operations_count}'
                        add_key(key)
                        
                        set_val(key, f'value_{operations_count}')
                        result = get_val(key)
                        
                        if result == f'value_{operations_count}':
                            operations_count += 1
                        else:
                            errors += 1
                    except Exception:
                        errors += 1
        
        actual_duration = time.time() - start_time
        throughput = operations_count / actual_duration
        error_rate = errors / (operations_count + errors) if (operations_count + errors) > 0 else 0
        
        print(f"Sustained load test: {throughput:.1f} ops/sec over {actual_duration:.1f}s")
        print(f"Total operations: {operations_count}, Errors: {errors} ({error_rate:.2%})")
        
        assert throughput > 5, f"Sustained throughput {throughput:.1f} ops/sec too low"
        assert error_rate < 0.05, f"Error rate {error_rate:.2%} too high"