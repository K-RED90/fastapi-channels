#!/usr/bin/env python3
"""Load testing script for WebSocket chat system.

Tests with thousands of concurrent connections and large group counts.
More realistic than extreme_load_test.py for actual testing.
"""

import asyncio
import json
import statistics
import time
from dataclasses import dataclass
from typing import Any

import httpx
import websockets
from websockets import ServerConnection, State


@dataclass
class LoadTestConfig:
    """Configuration for load testing"""

    server_url: str = "ws://localhost:8000"
    http_url: str = "http://localhost:8000"
    max_connections: int = 5000  # Realistic for single machine
    connections_per_second: int = 200
    test_duration: int = 120  # 2 minutes
    max_groups: int = 50000  # 50k groups
    message_rate: int = 500  # messages per second total
    message_size: int = 5000  # 5k characters per message


@dataclass
class LoadTestStats:
    """Statistics collected during load testing"""

    connections_established: int = 0
    connections_failed: int = 0
    messages_sent: int = 0
    messages_received: int = 0
    groups_created: int = 0
    response_times: list[float] | Any = None
    errors: list[str] | Any = None
    throughput_history: list[dict[str, float]] | Any = None

    def __post_init__(self):
        self.response_times = []
        self.errors = []
        self.throughput_history = []


class LoadTester:
    """Load tester for WebSocket connections with realistic limits"""

    def __init__(self, config: LoadTestConfig):
        self.config = config
        self.stats = LoadTestStats()
        self.connections: list[ServerConnection] = []
        self.is_running = True

    async def run_test(self) -> LoadTestStats:
        """Run the complete load test"""
        print("Starting load test...")
        print(f"Target: {self.config.max_connections} connections, {self.config.max_groups} groups")

        start_time = time.time()

        try:
            # Phase 1: Establish connections with rate limiting
            print("Phase 1: Establishing connections...")
            await self._establish_connections()

            # Phase 2: Create groups via HTTP (faster than WebSocket)
            print("Phase 2: Creating groups...")
            await self._create_groups_http()

            # Phase 3: Run messaging load test
            print("Phase 3: Running messaging load test...")
            await self._run_messaging_load()

        except Exception as e:
            self.stats.errors.append(f"Test error: {e}")
        finally:
            # Cleanup
            print("Phase 4: Cleaning up...")
            await self._cleanup()

        end_time = time.time()
        print(".2f")
        return self.stats

    async def _establish_connections(self):
        """Establish WebSocket connections with rate limiting"""
        semaphore = asyncio.Semaphore(self.config.connections_per_second)
        tasks = []

        for i in range(self.config.max_connections):

            async def connect_with_semaphore(user_id=i):
                async with semaphore:
                    await self._connect_user(user_id)

            tasks.append(asyncio.create_task(connect_with_semaphore()))

            # Report progress
            if (i + 1) % 500 == 0:
                print(f"  Initiated {i + 1}/{self.config.max_connections} connections")

        # Wait for all connections to complete
        await asyncio.gather(*tasks, return_exceptions=True)

        successful = len([c for c in self.connections if c and c.state != State.CLOSED])
        print(f"  Connections established: {successful}/{self.config.max_connections}")

    async def _connect_user(self, user_id: int):
        """Connect a single user with timeout and error handling"""
        try:
            uri = f"{self.config.server_url}/ws/user{user_id}"
            websocket = await asyncio.wait_for(
                websockets.connect(uri),
                timeout=30.0,  # Longer timeout for high load
            )
            self.connections.append(websocket)  # pyright: ignore[reportArgumentType]
            self.stats.connections_established += 1
        except Exception as e:
            self.stats.connections_failed += 1
            if len(self.stats.errors) < 100:  # Limit error collection
                self.stats.errors.append(f"Connection failed for user{user_id}: {e}")

    async def _create_groups_http(self):
        """Create groups via HTTP API for better performance"""
        async with httpx.AsyncClient() as client:
            semaphore = asyncio.Semaphore(100)  # Limit concurrent HTTP requests
            tasks = []

            for i in range(self.config.max_groups):

                async def create_with_semaphore(group_id=i):
                    async with semaphore:
                        await self._create_single_group_http(client, group_id)

                tasks.append(asyncio.create_task(create_with_semaphore()))

                # Report progress
                if (i + 1) % 5000 == 0:
                    print(f"  Initiated {i + 1}/{self.config.max_groups} groups")

            # Wait for all group creation to complete
            await asyncio.gather(*tasks, return_exceptions=True)

        print(f"  Groups created: {self.stats.groups_created}/{self.config.max_groups}")

    async def _create_single_group_http(self, client: httpx.AsyncClient, group_id: int):
        """Create a single group via HTTP API"""
        try:
            group_name = f"load_group_{group_id:06d}"  # Zero-padded for consistent naming
            url = f"{self.config.http_url}/api/rooms"

            data = {"name": group_name, "is_public": True}

            start_time = time.time()
            response = await client.post(url, json=data)
            end_time = time.time()

            if response.is_success:
                self.stats.groups_created += 1
                self.stats.response_times.append(end_time - start_time)
            elif len(self.stats.errors) < 1000:
                self.stats.errors.append(
                    f"Failed to create group {group_name}: {response.status_code}"
                )

        except Exception as e:
            if len(self.stats.errors) < 1000:
                self.stats.errors.append(f"Error creating group {group_id}: {e}")

    async def _run_messaging_load(self):
        """Run the messaging load test"""
        end_time = time.time() + self.config.test_duration
        last_report = time.time()

        print(f"  Running messaging test for {self.config.test_duration} seconds...")

        while time.time() < end_time and self.is_running:
            current_time = time.time()

            # Send messages at target rate
            await self._send_message_batch()

            # Report throughput every 10 seconds
            if current_time - last_report >= 10:
                throughput = {
                    "timestamp": current_time,
                    "messages_per_second": self.stats.messages_sent / (current_time - last_report),
                    "active_connections": len(
                        [c for c in self.connections if c and c.state != State.CLOSED]
                    ),
                }
                self.stats.throughput_history.append(throughput)
                print(
                    f"    Throughput: {throughput['messages_per_second']:.1f} msg/s, "
                    f"Active connections: {throughput['active_connections']}"
                )
                last_report = current_time

            # Small sleep to prevent busy waiting
            await asyncio.sleep(0.01)

        print(f"  Messaging test complete. Total messages: {self.stats.messages_sent}")

    async def _send_message_batch(self):
        """Send a batch of messages to maintain target rate"""
        if not self.connections:
            return

        # Calculate how many messages to send this batch
        target_batch_size = max(
            1, int(self.config.message_rate * 0.1)
        )  # Send 10% of rate per 100ms

        # Get active connections
        active_connections = [ws for ws in self.connections if ws and ws.state != State.CLOSED]
        if not active_connections:
            return

        # Send messages from random connections
        tasks = []
        for i in range(min(target_batch_size, len(active_connections))):
            ws = active_connections[i % len(active_connections)]
            tasks.append(asyncio.create_task(self._send_single_message(ws)))

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _send_single_message(self, websocket):
        """Send a single chat message"""
        try:
            message = {
                "type": "chat_message",
                "data": {"text": "x" * self.config.message_size, "room": "lobby"},
            }

            start_time = time.time()
            await asyncio.wait_for(websocket.send(json.dumps(message)), timeout=10.0)
            self.stats.messages_sent += 1

            # Try to receive response (own message echo)
            try:
                response = await asyncio.wait_for(websocket.recv(), timeout=5.0)
                response_data = json.loads(response)
                if response_data.get("type") == "chat_message":
                    self.stats.messages_received += 1
                    self.stats.response_times.append(time.time() - start_time)
            except TimeoutError:
                pass  # Message might not echo back

        except Exception as e:
            if len(self.stats.errors) < 100:
                self.stats.errors.append(f"Message send error: {e}")

    async def _cleanup(self):
        """Clean up all connections"""
        if self.connections:
            print(f"  Cleaning up {len(self.connections)} connections...")

            # Close connections in batches
            batch_size = 500
            for i in range(0, len(self.connections), batch_size):
                batch = self.connections[i : i + batch_size]
                tasks = []
                for ws in batch:
                    if ws and ws.state != State.CLOSED:
                        tasks.append(asyncio.create_task(ws.close()))

                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)
                    print(
                        f"    Closed {min(i + batch_size, len(self.connections))}/{len(self.connections)} connections"
                    )

        print("  Cleanup complete.")


def print_load_test_results(stats: LoadTestStats, config: LoadTestConfig):
    """Print comprehensive load test results"""
    print("\n" + "=" * 80)
    print("LOAD TEST RESULTS")
    print("=" * 80)

    print(f"Target Connections: {config.max_connections:,}")
    print(f"Target Groups: {config.max_groups:,}")
    print(f"Test Duration: {config.test_duration}s")
    print(f"Message Rate Target: {config.message_rate}/s")
    print(f"Message Size: {config.message_size:,} chars")

    print("\nACTUAL RESULTS:")
    print(f"Connections Established: {stats.connections_established:,}")
    print(f"Connections Failed: {stats.connections_failed:,}")
    print(
        f"Connection Success Rate: {stats.connections_established / max(1, config.max_connections) * 100:.1f}%"
    )
    print(f"Groups Created: {stats.groups_created:,}")
    print(
        f"Group Creation Success Rate: {stats.groups_created / max(1, config.max_groups) * 100:.1f}%"
    )
    print(f"Messages Sent: {stats.messages_sent:,}")
    print(f"Messages Received: {stats.messages_received:,}")
    print(
        f"Message Success Rate: {stats.messages_received / max(1, stats.messages_sent) * 100:.1f}%"
    )
    print(f"Errors Encountered: {len(stats.errors):,}")

    if stats.response_times:
        print("\nRESPONSE TIME STATISTICS:")
        print(f"  Average: {statistics.mean(stats.response_times):.3f}s")
        print(f"  Median: {statistics.median(stats.response_times):.3f}s")
        print(f"  Min: {min(stats.response_times):.3f}s")
        print(f"  Max: {max(stats.response_times):.3f}s")
        if len(stats.response_times) > 1:
            print(f"  95th percentile: {statistics.quantiles(stats.response_times, n=20)[18]:.3f}s")
            print(f"  99th percentile: {statistics.quantiles(stats.response_times, n=20)[19]:.3f}s")

    if stats.throughput_history:
        print("\nTHROUGHPUT HISTORY:")
        for entry in stats.throughput_history[-5:]:  # Show last 5 entries
            print(
                f"  {time.strftime('%H:%M:%S', time.localtime(entry['timestamp']))}: "
                f"{entry['messages_per_second']:.1f} msg/s, "
                f"{entry['active_connections']} active connections"
            )

    if stats.errors:
        print("\nSAMPLE ERRORS (first 10):")
        for error in stats.errors[:10]:
            print(f"  {error}")

    print("=" * 80)


async def main():
    """Main load test runner"""
    # Configuration for realistic load testing
    config = LoadTestConfig(
        server_url="ws://localhost:8000",
        http_url="http://localhost:8000",
        max_connections=5000,  # 5k connections - realistic for single machine
        connections_per_second=200,  # Ramp up gradually
        test_duration=120,  # 2 minutes
        max_groups=50000,  # 50k groups
        message_rate=500,  # 500 messages/second total
        message_size=5000,  # 5k character messages
    )

    print("LOAD TEST CONFIGURATION")
    print("=" * 50)
    print(f"Server URL: {config.server_url}")
    print(f"Max Connections: {config.max_connections:,}")
    print(f"Connections/second: {config.connections_per_second}")
    print(f"Test Duration: {config.test_duration}s")
    print(f"Max Groups: {config.max_groups:,}")
    print(f"Message Rate: {config.message_rate}/s")
    print(f"Message Size: {config.message_size:,} chars")
    print("=" * 50)

    # Run the load test
    tester = LoadTester(config)
    stats = await tester.run_test()

    # Print results
    print_load_test_results(stats, config)


if __name__ == "__main__":
    asyncio.run(main())
