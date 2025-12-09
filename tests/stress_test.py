#!/usr/bin/env python3
"""Stress testing script for WebSocket chat system.

Tests with high concurrent connections and large group counts.
Supports testing with thousands of connections and millions of groups.
"""

import asyncio
import json
import statistics
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any

import httpx
import websockets
from websockets import ClientConnection


@dataclass
class TestConfig:
    """Configuration for stress testing"""

    server_url: str = "ws://localhost:8000"
    http_url: str = "http://localhost:8000"
    max_connections: int = 1000  # Can be increased to 10k+ for real testing
    connections_per_second: int = 100
    test_duration: int = 60  # seconds
    max_groups: int = 10000  # Can be increased to 1M+ for real testing
    message_rate: int = 10  # messages per second per connection
    message_size: int = 1000  # characters per message


@dataclass
class TestStats:
    """Statistics collected during testing"""

    connections_established: int = 0
    connections_failed: int = 0
    messages_sent: int = 0
    messages_received: int = 0
    groups_created: int = 0
    response_times: list[float] | Any = None
    errors: list[str] | Any = None

    def __post_init__(self):
        self.response_times = []
        self.errors = []


class WebSocketLoadTester:
    """Load tester for WebSocket connections"""

    def __init__(self, config: TestConfig):
        self.config = config
        self.stats = TestStats()
        self.connections: list[ClientConnection] = []
        self.executor = ThreadPoolExecutor(max_workers=10)

    async def connect_user(self, user_id: int) -> ClientConnection | None:
        """Connect a single user"""
        try:
            uri = f"{self.config.server_url}/ws/user{user_id}"
            websocket = await websockets.connect(uri)
            self.connections.append(websocket)
            self.stats.connections_established += 1
            return websocket
        except Exception as e:
            self.stats.connections_failed += 1
            self.stats.errors.append(f"Connection failed for user{user_id}: {e}")
            return None

    async def create_groups(self, count: int):
        """Create many groups/rooms"""
        if not self.connections:
            return

        # Use first connection to create groups
        ws = self.connections[0]

        for i in range(count):
            try:
                group_name = f"stress_group_{i}"
                message = {
                    "type": "create_room",
                    "data": {"room_name": group_name, "is_public": True},
                }

                start_time = time.time()
                await ws.send(json.dumps(message))

                # Wait for response
                response = await asyncio.wait_for(ws.recv(), timeout=5.0)
                end_time = time.time()

                response_data = json.loads(response)
                if response_data.get("type") == "room_created":
                    self.stats.groups_created += 1
                    self.stats.response_times.append(end_time - start_time)
                else:
                    self.stats.errors.append(f"Failed to create group {group_name}")

            except Exception as e:
                self.stats.errors.append(f"Error creating group {i}: {e}")

    async def send_messages(self, duration: int):
        """Send messages from all connections"""
        end_time = time.time() + duration

        while time.time() < end_time and self.connections:
            tasks = []

            # Send messages from a subset of connections
            active_connections = self.connections[: min(len(self.connections), 100)]

            for ws in active_connections:
                if len(tasks) >= self.config.message_rate:
                    break

                task = asyncio.create_task(self._send_single_message(ws))
                tasks.append(task)

            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
                await asyncio.sleep(1.0 / self.config.message_rate)  # Rate limiting

    async def _send_single_message(self, websocket):
        """Send a single message from a connection"""
        try:
            message = {
                "type": "chat_message",
                "data": {
                    "text": "x" * self.config.message_size,
                    "room": "lobby",  # Send to default room
                },
            }

            start_time = time.time()
            await websocket.send(json.dumps(message))
            self.stats.messages_sent += 1

            # Try to receive response (own message echo)
            try:
                response = await asyncio.wait_for(websocket.recv(), timeout=2.0)
                response_data = json.loads(response)
                if response_data.get("type") == "chat_message":
                    self.stats.messages_received += 1
                    self.stats.response_times.append(time.time() - start_time)
            except TimeoutError:
                pass  # Message might not echo back immediately

        except Exception as e:
            self.stats.errors.append(f"Error sending message: {e}")

    async def run_test(self) -> TestStats:
        """Run the complete stress test"""
        print(f"Starting stress test with config: {self.config}")

        # Phase 1: Establish connections
        print(f"Phase 1: Establishing {self.config.max_connections} connections...")
        connection_tasks = []

        for i in range(self.config.max_connections):
            task = asyncio.create_task(self.connect_user(i))
            connection_tasks.append(task)

            # Rate limit connection establishment
            if len(connection_tasks) >= self.config.connections_per_second:
                await asyncio.gather(*connection_tasks, return_exceptions=True)
                connection_tasks = []
                await asyncio.sleep(1.0)

        # Complete remaining connections
        if connection_tasks:
            await asyncio.gather(*connection_tasks, return_exceptions=True)

        print(f"Connections established: {self.stats.connections_established}")
        print(f"Connections failed: {self.stats.connections_failed}")

        # Phase 2: Create groups
        if self.config.max_groups > 0:
            print(f"Phase 2: Creating {self.config.max_groups} groups...")
            await self.create_groups(self.config.max_groups)
            print(f"Groups created: {self.stats.groups_created}")

        # Phase 3: Send messages
        print(f"Phase 3: Sending messages for {self.config.test_duration} seconds...")
        await self.send_messages(self.config.test_duration)

        print(f"Messages sent: {self.stats.messages_sent}")
        print(f"Messages received: {self.stats.messages_received}")

        # Cleanup
        print("Phase 4: Cleaning up connections...")
        close_tasks = [ws.close() for ws in self.connections if ws]
        await asyncio.gather(*close_tasks, return_exceptions=True)

        return self.stats


class HTTPGroupTester:
    """Test group creation via HTTP API"""

    def __init__(self, config: TestConfig):
        self.config = config
        self.stats = TestStats()

    async def create_groups_http(self, count: int):
        """Create groups via HTTP API"""
        async with httpx.AsyncClient() as client:
            tasks = []

            for i in range(count):
                task = asyncio.create_task(self._create_single_group(client, i))
                tasks.append(task)

                # Batch requests
                if len(tasks) >= 50:
                    await asyncio.gather(*tasks, return_exceptions=True)
                    tasks = []

            # Complete remaining tasks
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)

    async def _create_single_group(self, session: httpx.AsyncClient, group_id: int):
        """Create a single group via HTTP"""
        try:
            group_name = f"http_group_{group_id}"
            url = f"{self.config.http_url}/api/rooms"

            data = {"name": group_name, "is_public": True}

            start_time = time.time()
            response = await session.post(url, json=data)
            end_time = time.time()

            if response.is_success:
                self.stats.groups_created += 1
                self.stats.response_times.append(end_time - start_time)
            else:
                self.stats.errors.append(
                    f"Failed to create group {group_name}: {response.status_code}"
                )

        except Exception as e:
            self.stats.errors.append(f"Error creating group {group_id}: {e}")


def print_stats(stats: TestStats):
    """Print test statistics"""
    print("\n" + "=" * 50)
    print("TEST RESULTS")
    print("=" * 50)

    print(f"Connections established: {stats.connections_established}")
    print(f"Connections failed: {stats.connections_failed}")
    print(f"Groups created: {stats.groups_created}")
    print(f"Messages sent: {stats.messages_sent}")
    print(f"Messages received: {stats.messages_received}")
    print(f"Errors encountered: {len(stats.errors)}")

    if stats.response_times:
        print("Response Time Statistics:")
        print(f"  Average: {statistics.mean(stats.response_times):.3f}s")
        print(f"  Median: {statistics.median(stats.response_times):.3f}s")
        print(f"  Min: {min(stats.response_times):.3f}s")
        print(f"  Max: {max(stats.response_times):.3f}s")
        print(f"  95th percentile: {statistics.quantiles(stats.response_times, n=20)[18]:.3f}s")

    if stats.errors:
        print("\nFirst 10 errors:")
        for error in stats.errors[:10]:
            print(f"  {error}")

    print("=" * 50)


async def main():
    """Main test runner"""
    # Configuration for high-load testing
    config = TestConfig(
        server_url="ws://localhost:8000",
        http_url="http://localhost:8000",
        max_connections=5000,  # Scale this up to 100k+ on powerful machines
        connections_per_second=500,
        test_duration=30,
        max_groups=50000,  # Scale this up to 3M+ on powerful machines
        message_rate=50,
        message_size=5000,
    )

    print("Starting high-load stress test...")
    print(f"Target: {config.max_connections} connections, {config.max_groups} groups")

    # WebSocket load testing
    ws_tester = WebSocketLoadTester(config)
    ws_stats = await ws_tester.run_test()

    # HTTP group creation testing (optional)
    if config.max_groups > 10000:  # Only do HTTP testing for very large group counts
        print("\nStarting HTTP group creation test...")
        http_tester = HTTPGroupTester(config)
        await http_tester.create_groups_http(config.max_groups)
        ws_stats.groups_created += http_tester.stats.groups_created
        ws_stats.response_times.extend(http_tester.stats.response_times)
        ws_stats.errors.extend(http_tester.stats.errors)

    print_stats(ws_stats)


if __name__ == "__main__":
    # Run with proper event loop
    asyncio.run(main())
