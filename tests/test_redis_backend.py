# pyright: reportMissingImports=false

import asyncio
import uuid

import pytest
import pytest_asyncio
from redis.asyncio import Redis

from fastapi_channels.backends import RedisBackend


@pytest_asyncio.fixture(scope="module")
async def redis_url() -> str:
    """Ensure Redis is available for tests."""
    url = "redis://localhost:6379/0"
    client = Redis.from_url(url)
    try:
        await client.execute_command("PING")
    except Exception:
        await client.close()
        pytest.skip("Redis server not available for Redis backend tests")
    await client.close()
    return url


@pytest_asyncio.fixture
async def backend_factory(redis_url: str):
    """Factory fixture to create isolated RedisBackend instances."""

    async def _make_backend(
        registry_expiry: int | None = 30,
        group_expiry: int | None = 30,
        channel_prefix: str | None = None,
    ):
        prefix = channel_prefix or f"test:ws:{uuid.uuid4().hex}:"
        backend = RedisBackend(
            redis_url=redis_url,
            channel_prefix=prefix,
            registry_expiry=registry_expiry,
            group_expiry=group_expiry,
        )
        await backend.connect()
        await backend.flush()
        return backend

    yield _make_backend


@pytest.mark.asyncio
async def test_cleanup_stale_connections_by_server_id(backend_factory):
    """Connections from other servers should be removed."""
    backend = await backend_factory(registry_expiry=30)
    try:
        await backend.registry_add_connection(
            connection_id="conn-keep",
            user_id="user-keep",
            metadata={"server_instance_id": "srv-a"},
            groups=set(),
            heartbeat_timeout=30,
        )
        await backend.registry_add_connection(
            connection_id="conn-stale",
            user_id="user-stale",
            metadata={"server_instance_id": "srv-b"},
            groups={"room1"},
            heartbeat_timeout=30,
        )

        stats = await backend.cleanup_stale_connections(server_instance_id="srv-a")

        assert stats["connections_removed"] == 1
        assert await backend.registry_count_connections() == 1
        remaining = await backend.registry_get_user_connections("user-keep")
        assert "conn-keep" in remaining
    finally:
        await backend.flush()
        await backend.cleanup()


@pytest.mark.asyncio
async def test_cleanup_stale_connections_by_ttl(backend_factory):
    """Expired TTL entries should be removed even for same server."""
    backend = await backend_factory(registry_expiry=1)
    try:
        await backend.registry_add_connection(
            connection_id="conn-expired",
            user_id="user-expired",
            metadata={"server_instance_id": "srv-a"},
            groups=set(),
            heartbeat_timeout=30,
        )

        # Remove TTLs to simulate missing/expired TTL metadata
        conn_key = f"{backend.channel_prefix}registry:connection:conn-expired"
        connections_key = f"{backend.channel_prefix}registry:connections"
        await backend.redis.persist(conn_key)
        await backend.redis.persist(connections_key)

        stats = await backend.cleanup_stale_connections(server_instance_id="srv-a")

        assert stats["connections_removed"] == 1
        assert await backend.registry_count_connections() == 0
    finally:
        await backend.flush()
        await backend.cleanup()


@pytest.mark.asyncio
async def test_cleanup_preserves_active_connections(backend_factory):
    """Active connections for current server should remain."""
    backend = await backend_factory(registry_expiry=30)
    try:
        await backend.registry_add_connection(
            connection_id="conn-active",
            user_id="user-active",
            metadata={"server_instance_id": "srv-a"},
            groups={"room-a"},
            heartbeat_timeout=30,
        )

        stats = await backend.cleanup_stale_connections(server_instance_id="srv-a")

        assert stats["connections_removed"] == 0
        assert await backend.registry_count_connections() == 1
    finally:
        await backend.flush()
        await backend.cleanup()


@pytest.mark.asyncio
async def test_cleanup_orphaned_group_members(backend_factory):
    backend = await backend_factory(registry_expiry=30, group_expiry=30)
    try:
        await backend.registry_add_connection(
            connection_id="conn-valid",
            user_id="user-valid",
            metadata={"server_instance_id": "srv-clean"},
            groups=set(),
            heartbeat_timeout=30,
        )
        await backend.group_add("room", "conn-valid")

        # Add orphaned members
        await backend.redis.sadd(f"{backend.channel_prefix}group:room", "ghost")
        await backend.redis.sadd(f"{backend.channel_prefix}group:orphan", "ghost2")

        stats = await backend.cleanup_orphaned_group_members()

        members = await backend.group_channels("room")
        assert "conn-valid" in members
        assert "ghost" not in members

        # Orphan-only group should be removed
        orphan_exists = await backend.redis.exists(f"{backend.channel_prefix}group:orphan")
        assert orphan_exists == 0

        assert stats["orphaned_members_removed"] >= 1
        assert stats["empty_groups_removed"] >= 1
    finally:
        await backend.flush()
        await backend.cleanup()


@pytest.mark.asyncio
async def test_cleanup_empty_groups(backend_factory):
    """Groups with only orphaned members are removed."""
    backend = await backend_factory(registry_expiry=30, group_expiry=30)
    try:
        await backend.redis.sadd(f"{backend.channel_prefix}group:lonely", "ghost")

        stats = await backend.cleanup_orphaned_group_members()

        exists = await backend.redis.exists(f"{backend.channel_prefix}group:lonely")
        assert exists == 0
        assert stats["empty_groups_removed"] >= 1
    finally:
        await backend.flush()
        await backend.cleanup()


@pytest.mark.asyncio
async def test_concurrent_cleanup_safe(backend_factory):
    """Concurrent cleanup executions should be safe and idempotent."""
    shared_prefix = f"test:ws:{uuid.uuid4().hex}:"
    backend_a = await backend_factory(
        registry_expiry=1, group_expiry=30, channel_prefix=shared_prefix
    )
    backend_b = await backend_factory(
        registry_expiry=1, group_expiry=30, channel_prefix=shared_prefix
    )

    try:
        await backend_a.registry_add_connection(
            connection_id="conn-stale",
            user_id="user-concurrent",
            metadata={"server_instance_id": "srv-old"},
            groups=set(),
            heartbeat_timeout=30,
        )

        conn_key = f"{shared_prefix}registry:connection:conn-stale"
        connections_key = f"{shared_prefix}registry:connections"
        await backend_a.redis.persist(conn_key)
        await backend_a.redis.persist(connections_key)

        results = await asyncio.gather(
            backend_a.cleanup_stale_connections(server_instance_id="srv-a"),
            backend_b.cleanup_stale_connections(server_instance_id="srv-b"),
        )

        total_removed = sum(r["connections_removed"] for r in results)
        assert total_removed >= 1
        assert await backend_a.registry_count_connections() == 0
    finally:
        await backend_a.flush()
        await backend_b.flush()
        await backend_a.cleanup()
        await backend_b.cleanup()
