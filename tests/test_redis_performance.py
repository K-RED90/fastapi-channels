import asyncio
import logging
import statistics
import time
from unittest.mock import patch

import pytest

from fastapi_channels.backends import RedisBackend

logger = logging.getLogger(__name__)


class TestRedisPerformance:
    """Performance tests for Redis backend under load"""

    @pytest.fixture
    async def redis_backend(self):
        """Create RedisBackend instance for performance testing"""
        with patch("fastapi_channel.backends.redis.Redis") as mock_redis_class:
            mock_redis = mock_redis_class.return_value
            # Configure mock to simulate Redis behavior with timing
            mock_redis.get.return_value = None
            mock_redis.set.return_value = True
            mock_redis.delete.return_value = 1
            mock_redis.sadd.return_value = 1
            mock_redis.srem.return_value = 1
            mock_redis.smembers.return_value = set()
            mock_redis.scard.return_value = 0
            mock_redis.publish.return_value = 1
            mock_redis.flushdb.return_value = True

            backend = RedisBackend(redis_url="redis://localhost:6379/0", max_connections=200000)
            backend._redis = mock_redis
            yield backend

    @pytest.mark.asyncio
    async def test_redis_group_operations_performance(self, redis_backend):
        """Test Redis group operations performance with many operations"""
        # Test adding many channels to groups
        group_count = 100
        channels_per_group = 100
        total_operations = group_count * channels_per_group

        logger.info(f"Testing {total_operations} group add operations...")

        start_time = time.time()

        # Create groups with many channels
        for group_id in range(group_count):
            group_name = f"perf_group_{group_id}"
            for channel_id in range(channels_per_group):
                channel_name = f"channel_{group_id}_{channel_id}"
                await redis_backend.group_add(group_name, channel_name)

        end_time = time.time()
        total_time = end_time - start_time
        ops_per_second = total_operations / total_time

        logger.info(f"Total time: {total_time:.2f} seconds")
        logger.info(f"Operations per second: {ops_per_second:.1f}")
        # Performance assertion - should be fast with Redis
        assert ops_per_second > 1000  # At least 1000 operations per second

        # Verify some groups
        for group_id in range(min(5, group_count)):
            group_name = f"perf_group_{group_id}"
            channels = await redis_backend.group_channels(group_name)
            assert len(channels) == channels_per_group

    @pytest.mark.asyncio
    async def test_redis_registry_operations_performance(self, redis_backend):
        """Test Redis registry operations performance"""
        user_count = 1000
        connections_per_user = 5
        total_operations = user_count * connections_per_user

        logger.info(f"Testing {total_operations} registry operations...")

        start_time = time.time()

        # Add many connections
        for user_id in range(user_count):
            for conn_id in range(connections_per_user):
                await redis_backend.registry_add_connection_if_under_limit(
                    f"user_{user_id}", f"conn_{conn_id}", max_connections=10
                )

        end_time = time.time()
        total_time = end_time - start_time
        ops_per_second = total_operations / total_time

        logger.info(f"Total time: {total_time:.2f} seconds")
        logger.info(f"Operations per second: {ops_per_second:.1f}")
        # Performance assertion
        assert ops_per_second > 1000

        # Verify some connections exist
        for user_id in range(min(10, user_count)):
            for conn_id in range(connections_per_user):
                exists = await redis_backend.registry_connection_exists(
                    f"user_{user_id}", f"conn_{conn_id}"
                )
                assert exists is True

    @pytest.mark.asyncio
    async def test_redis_concurrent_operations(self, redis_backend):
        """Test Redis performance under concurrent operations"""
        concurrent_tasks = 100
        operations_per_task = 50
        total_operations = concurrent_tasks * operations_per_task

        logger.info(f"Testing {total_operations} concurrent operations...")

        async def perform_operations(task_id: int):
            """Perform operations for a single task"""
            results = []
            for i in range(operations_per_task):
                # Mix of different operations
                if i % 3 == 0:
                    # Group operation
                    await redis_backend.group_add(f"conc_group_{task_id}", f"channel_{i}")
                    results.append("group_add")
                elif i % 3 == 1:
                    # Registry operation
                    await redis_backend.registry_add_connection_if_under_limit(
                        f"conc_user_{task_id}", f"conc_conn_{i}", max_connections=10
                    )
                    results.append("registry_add")
                else:
                    # Send operation
                    await redis_backend.send_to_group(
                        f"conc_group_{task_id}", {"type": "test", "data": f"message_{i}"}
                    )
                    results.append("send")
            return results

        start_time = time.time()

        # Run concurrent tasks
        tasks = [perform_operations(i) for i in range(concurrent_tasks)]
        results = await asyncio.gather(*tasks)

        end_time = time.time()
        total_time = end_time - start_time
        ops_per_second = total_operations / total_time

        logger.info(f"Total time: {total_time:.2f} seconds")
        logger.info(f"Operations per second: {ops_per_second:.1f}")
        # Verify results
        assert len(results) == concurrent_tasks
        for task_results in results:
            assert len(task_results) == operations_per_task

        # Performance assertion
        assert ops_per_second > 500  # Should handle concurrent load well

    @pytest.mark.asyncio
    async def test_redis_broadcast_performance(self, redis_backend):
        """Test Redis broadcast performance to many subscribers"""
        group_name = "broadcast_perf_group"
        subscriber_count = 5000  # Simulate many subscribers

        logger.info(f"Testing broadcast to {subscriber_count} subscribers...")

        # Add many subscribers to group
        start_setup = time.time()
        for i in range(subscriber_count):
            await redis_backend.group_add(group_name, f"subscriber_{i}")
        setup_time = time.time() - start_setup

        logger.info("Setup time: %s seconds", setup_time)
        # Send broadcast message
        message = {
            "type": "broadcast",
            "data": {
                "text": "System-wide announcement: Server maintenance scheduled",
                "priority": "high",
                "timestamp": int(time.time()),
            },
        }

        start_broadcast = time.time()
        await redis_backend.send_to_group(group_name, message)
        broadcast_time = time.time() - start_broadcast

        logger.info("Broadcast time: %s seconds", broadcast_time)
        # Broadcast should be very fast with Redis pub/sub
        assert broadcast_time < 0.1  # Under 100ms for Redis broadcast

        # Setup should also be reasonable
        setup_ops_per_second = subscriber_count / setup_time
        logger.info("Setup operations per second: %s", setup_ops_per_second)
        assert setup_ops_per_second > 1000

    @pytest.mark.asyncio
    async def test_redis_large_group_management(self, redis_backend):
        """Test managing very large groups (thousands of channels)"""
        group_name = "large_group"
        channel_count = 10000  # 10k channels in one group

        logger.info("Testing large group with %s channels...", channel_count)

        # Add channels to group
        start_time = time.time()
        for i in range(channel_count):
            await redis_backend.group_add(group_name, f"channel_{i:05d}")
        add_time = time.time() - start_time

        logger.info("Add time: %s seconds", add_time)
        logger.info("Add operations per second: %s", channel_count / add_time)
        # Retrieve all channels
        start_retrieve = time.time()
        channels = await redis_backend.group_channels(group_name)
        retrieve_time = time.time() - start_retrieve

        logger.info("Retrieve time: %s seconds", retrieve_time)
        assert len(channels) == channel_count

        # Verify some channels
        for i in range(0, channel_count, 1000):  # Check every 1000th channel
            channel_name = f"channel_{i:05d}"
            assert channel_name in channels

        # Test removing channels
        remove_count = 1000
        start_remove = time.time()
        for i in range(remove_count):
            await redis_backend.group_discard(group_name, f"channel_{i:05d}")
        remove_time = time.time() - start_remove

        logger.info("Remove time: %s seconds", remove_time)
        logger.info("Remove operations per second: %s", remove_count / remove_time)
        # Verify channels were removed
        remaining_channels = await redis_backend.group_channels(group_name)
        assert len(remaining_channels) == channel_count - remove_count

        # Performance assertions
        assert add_time < 10.0  # Should complete within 10 seconds
        assert retrieve_time < 1.0  # Should retrieve quickly
        assert remove_time < 2.0  # Should remove reasonably fast

    @pytest.mark.asyncio
    async def test_redis_connection_scaling_simulation(self, redis_backend):
        """Simulate scaling connections like in the load tests"""
        # Simulate the connection patterns from load_test.py
        total_connections = 1000  # Scaled down for unit test
        users_per_batch = 50

        logger.info("Simulating %s connections across multiple users...", total_connections)

        start_time = time.time()

        for batch_start in range(0, total_connections, users_per_batch):
            batch_end = min(batch_start + users_per_batch, total_connections)

            # Simulate connections for this batch of users
            tasks = []
            for user_id in range(batch_start, batch_end):
                # Each user gets a few connections
                for conn_num in range(3):
                    task = redis_backend.registry_add_connection_if_under_limit(
                        f"user_{user_id}", f"conn_{conn_num}", max_connections=5
                    )
                    tasks.append(task)

            await asyncio.gather(*tasks)

        end_time = time.time()
        total_time = end_time - start_time
        connections_per_second = total_connections * 3 / total_time  # 3 connections per user

        logger.info("Total time: %s seconds", total_time)
        logger.info("Connections per second: %s", connections_per_second)
        # Verify connections
        total_verified = 0
        for user_id in range(min(10, total_connections)):  # Verify subset
            for conn_num in range(3):
                exists = await redis_backend.registry_connection_exists(
                    f"user_{user_id}", f"conn_{conn_num}"
                )
                if exists:
                    total_verified += 1

        assert total_verified > 0

        # Performance assertion - Redis should handle connection scaling well
        expect_connections_per_second = 500
        assert connections_per_second > expect_connections_per_second

    @pytest.mark.asyncio
    async def test_redis_memory_efficiency_simulation(self, redis_backend):
        """Test Redis memory efficiency with many small operations"""
        # Simulate creating many small groups and messages
        group_count = 1000
        messages_per_group = 10

        logger.info(
            "Testing memory efficiency with %s groups, %s messages each...",
            group_count,
            messages_per_group,
        )

        start_time = time.time()

        for group_id in range(group_count):
            group_name = f"mem_group_{group_id}"

            # Add a channel to each group
            await redis_backend.group_add(group_name, f"channel_{group_id}")

            # Send messages to group
            for msg_id in range(messages_per_group):
                message = {
                    "type": "chat",
                    "data": {
                        "text": f"Message {msg_id} in group {group_id}",
                        "user": f"user_{group_id}",
                    },
                }
                await redis_backend.send_to_group(group_name, message)

        end_time = time.time()
        total_time = end_time - start_time
        total_operations = group_count * (1 + messages_per_group)  # groups + messages
        ops_per_second = total_operations / total_time

        logger.info("Total time: %s seconds", total_time)
        logger.info("Operations per second: %s", ops_per_second)
        # Redis should handle many small operations efficiently
        expect_ops_per_second = 2000
        assert ops_per_second > expect_ops_per_second

    @pytest.mark.asyncio
    async def test_redis_latency_under_load(self, redis_backend):
        """Test Redis latency remains low under load"""
        operation_count = 1000
        latencies: list[float] = []

        logger.info("Testing latency with %s operations...", operation_count)

        for i in range(operation_count):
            start_time = time.time()

            # Alternate between different operations
            if i % 3 == 0:
                await redis_backend.group_add(f"latency_group_{i % 10}", f"channel_{i}")
            elif i % 3 == 1:
                await redis_backend.registry_add_connection_if_under_limit(
                    f"latency_user_{i % 20}", f"conn_{i}", max_connections=10
                )
            else:
                await redis_backend.send_to_group(
                    f"latency_group_{i % 10}", {"type": "ping", "data": f"msg_{i}"}
                )

            end_time = time.time()
            latencies.append(end_time - start_time)

        # Calculate latency statistics
        avg_latency = statistics.mean(latencies)
        median_latency = statistics.median(latencies)
        p95_latency = statistics.quantiles(latencies, n=20)[18]
        p99_latency = statistics.quantiles(latencies, n=20)[19]

        logger.info("Latency Statistics:")
        logger.info("Average latency: %s seconds", avg_latency)
        logger.info("Median latency: %s seconds", median_latency)
        logger.info("P95 latency: %s seconds", p95_latency)
        logger.info("P99 latency: %s seconds", p99_latency)
        # Redis should maintain low latency even under load
        avg_10ms = 0.01
        p95_50ms = 0.05
        p99_100ms = 0.1
        assert avg_latency < avg_10ms  # Under 10ms average
        assert p95_latency < p95_50ms  # Under 50ms p95
        assert p99_latency < p99_100ms  # Under 100ms p99


if __name__ == "__main__":
    pytest.main([__file__])
