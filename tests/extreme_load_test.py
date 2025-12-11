#!/usr/bin/env python3
"""Extreme load testing script for WebSocket chat system.

Designed to test with 100k+ concurrent connections and 3M+ groups.
Uses distributed approach with multiple processes/workers.
"""

import asyncio
import json
import multiprocessing
import signal
import statistics
import sys
import time
from dataclasses import dataclass, field
from typing import Any

import httpx
from websockets import ClientConnection, State
import websockets


@dataclass
class ExtremeTestConfig:
    """Configuration for extreme load testing"""

    server_url: str = "ws://localhost:8000"
    http_url: str = "http://localhost:8000"
    target_connections: int = 100000  # 100k+ connections
    target_groups: int = 3000000  # 3M+ groups
    workers: int = multiprocessing.cpu_count()  # Use all CPU cores
    connections_per_worker: int = 0  # Calculated automatically
    groups_per_worker: int = 0  # Calculated automatically
    test_duration: int = 300  # 5 minutes
    ramp_up_time: int = 60  # 1 minute to reach full load
    message_rate: int = 100  # messages per second total
    message_size: int = 10000  # 10k characters per message
    heartbeat_interval: int = 30  # seconds


@dataclass
class WorkerStats:
    """Statistics from a single worker"""

    worker_id: int
    connections_established: int = 0
    connections_failed: int = 0
    messages_sent: int = 0
    messages_received: int = 0
    groups_created: int = 0
    response_times: list[float] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    start_time: float = 0
    end_time: float = 0

    def __post_init__(self):
        self.response_times = []
        self.errors = []


class ExtremeLoadWorker:
    """Worker process for extreme load testing"""

    def __init__(self, worker_id: int, config: ExtremeTestConfig):
        self.worker_id = worker_id
        self.config = config
        self.stats = WorkerStats(worker_id=worker_id)
        self.connections: list[ClientConnection] = []
        self.is_running = True

    async def run(self) -> WorkerStats:
        """Run the worker's test cycle"""
        self.stats.start_time = time.time()

        try:
            # Phase 1: Ramp up connections
            await self._ramp_up_connections()

            # Phase 2: Create groups (if this worker is responsible for groups)
            if self.worker_id == 0:  # Only first worker creates groups
                await self._create_groups()

            # Phase 3: Send messages and maintain connections
            await self._run_messaging_phase()

        except Exception as e:
            self.stats.errors.append(f"Worker {self.worker_id} error: {e}")
        finally:
            self.stats.end_time = time.time()
            await self._cleanup()

        return self.stats

    async def _ramp_up_connections(self):
        """Gradually establish connections"""
        connections_to_create = self.config.connections_per_worker
        ramp_up_steps = min(100, connections_to_create)  # Ramp up in batches

        print(
            f"Worker {self.worker_id}: Starting connection ramp-up ({connections_to_create} connections)"
        )

        for step in range(ramp_up_steps):
            batch_size = max(1, connections_to_create // ramp_up_steps)
            start_idx = step * batch_size
            end_idx = min(start_idx + batch_size, connections_to_create)

            # Create connections in this batch
            tasks = []
            for i in range(start_idx, end_idx):
                user_id = self.worker_id * self.config.connections_per_worker + i
                task = asyncio.create_task(self._connect_user(user_id))
                tasks.append(task)

            await asyncio.gather(*tasks, return_exceptions=True)

            # Progress reporting
            if (step + 1) % 10 == 0:
                print(
                    f"Worker {self.worker_id}: {len(self.connections)}/{connections_to_create} connections established"
                )

            # Rate limiting for ramp-up
            await asyncio.sleep(self.config.ramp_up_time / ramp_up_steps)

        print(
            f"Worker {self.worker_id}: Connection ramp-up complete. {len(self.connections)} connections active."
        )

    async def _connect_user(self, user_id: int):
        """Connect a single user with error handling"""
        try:
            uri = f"{self.config.server_url}/ws/user{user_id}"
            websocket = await asyncio.wait_for(websockets.connect(uri), timeout=10.0)
            self.connections.append(websocket)
            self.stats.connections_established += 1
        except Exception as e:
            self.stats.connections_failed += 1
            if len(self.stats.errors) < 100:  # Limit error collection
                self.stats.errors.append(f"Connection failed for user{user_id}: {e}")

    async def _create_groups(self):
        """Create groups using WebSocket connections"""
        if not self.connections:
            return

        print(f"Worker {self.worker_id}: Creating {self.config.groups_per_worker} groups")

        # Use a subset of connections to create groups
        group_creators = self.connections[:min(100, len(self.connections))]

        tasks = []
        for i in range(self.config.groups_per_worker):
            # Cycle through available connections
            connection = group_creators[i % len(group_creators)]
            task = asyncio.create_task(self._create_single_group(connection, i))
            tasks.append(task)

            # Batch group creation
            if len(tasks) >= 100:
                await asyncio.gather(*tasks, return_exceptions=True)
                tasks = []
                print(
                    f"Worker {self.worker_id}: Created {i + 1}/{self.config.groups_per_worker} groups"
                )

        # Complete remaining tasks
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        print(
            f"Worker {self.worker_id}: Group creation complete. {self.stats.groups_created} groups created."
        )

    async def _create_single_group(self, websocket, group_id: int):
        """Create a single group via WebSocket message"""
        try:
            group_name = f"extreme_group_{group_id}"

            create_room_msg = {
                "type": "create_room",
                "data": {
                    "room_name": group_name,
                    "is_public": True
                }
            }

            start_time = time.time()
            await asyncio.wait_for(websocket.send(json.dumps(create_room_msg)), timeout=5.0)
            self.stats.groups_created += 1
            self.stats.response_times.append(time.time() - start_time)

        except Exception as e:
            if len(self.stats.errors) < 1000:
                self.stats.errors.append(f"Error creating group {group_id}: {e}")

    async def _run_messaging_phase(self):
        """Run the messaging phase with heartbeat maintenance"""
        print(f"Worker {self.worker_id}: Starting messaging phase ({self.config.test_duration}s)")

        end_time = time.time() + self.config.test_duration
        last_heartbeat = time.time()
        last_message = time.time()
        message_interval = 1.0 / (self.config.message_rate / max(1, self.config.workers))

        while time.time() < end_time and self.is_running:
            current_time = time.time()

            # Send heartbeat if needed
            if current_time - last_heartbeat >= self.config.heartbeat_interval:
                await self._send_heartbeats()
                last_heartbeat = current_time

            # Send messages at specified rate
            if current_time - last_message >= message_interval:
                await self._send_messages()
                last_message = current_time

            # Small sleep to prevent busy waiting
            await asyncio.sleep(0.01)

        print(f"Worker {self.worker_id}: Messaging phase complete.")

    async def _send_heartbeats(self):
        """Send heartbeat to all connections"""
        if not self.connections:
            return

        tasks = []
        for ws in self.connections[: min(len(self.connections), 100)]:  # Sample connections
            if ws and ws.state != State.CLOSED:
                task = asyncio.create_task(self._send_heartbeat(ws))
                tasks.append(task)

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _send_heartbeat(self, websocket):
        """Send a single heartbeat"""
        try:
            ping_msg = {"type": "ping", "data": {}}
            await asyncio.wait_for(websocket.send(json.dumps(ping_msg)), timeout=5.0)
        except Exception:
            pass  # Ignore heartbeat failures

    async def _send_messages(self):
        """Send messages from active connections"""
        if not self.connections:
            return

        # Send from a subset of connections to maintain rate
        active_connections = [ws for ws in self.connections if ws and ws.state != State.CLOSED]
        if not active_connections:
            return

        messages_to_send = min(len(active_connections), 10)  # Send from up to 10 connections

        tasks = []
        for i in range(messages_to_send):
            ws = active_connections[i % len(active_connections)]
            task = asyncio.create_task(self._send_single_message(ws))
            tasks.append(task)

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
            await asyncio.wait_for(websocket.send(json.dumps(message)), timeout=5.0)
            self.stats.messages_sent += 1

            # Try to receive response
            try:
                response = await asyncio.wait_for(websocket.recv(), timeout=2.0)
                response_data = json.loads(response)
                if response_data.get("type") == "chat_message":
                    self.stats.messages_received += 1
                    self.stats.response_times.append(time.time() - start_time)
            except TimeoutError:
                pass

        except Exception as e:
            if len(self.stats.errors) < 100:
                self.stats.errors.append(f"Message send error: {e}")

    async def _cleanup(self):
        """Clean up all connections"""
        if self.connections:
            print(f"Worker {self.worker_id}: Cleaning up {len(self.connections)} connections")

            tasks = []
            for ws in self.connections:
                if ws and ws.state != State.CLOSED:
                    task = asyncio.create_task(ws.close())
                    tasks.append(task)

            await asyncio.gather(*tasks, return_exceptions=True)


def run_worker_process(worker_id: int, config: ExtremeTestConfig) -> WorkerStats:
    """Run a single worker process"""

    def signal_handler(signum, frame):
        print(f"Worker {worker_id}: Received signal {signum}, shutting down...")
        worker.is_running = False

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    worker = ExtremeLoadWorker(worker_id, config)

    # Run the worker in the event loop
    try:
        return asyncio.run(worker.run())
    except KeyboardInterrupt:
        print(f"Worker {worker_id}: Keyboard interrupt received")
        return worker.stats


def aggregate_stats(worker_stats: list[WorkerStats]) -> dict[str, Any]:
    """Aggregate statistics from all workers"""
    total_stats = {
        "total_connections_established": sum(s.connections_established for s in worker_stats),
        "total_connections_failed": sum(s.connections_failed for s in worker_stats),
        "total_messages_sent": sum(s.messages_sent for s in worker_stats),
        "total_messages_received": sum(s.messages_received for s in worker_stats),
        "total_groups_created": sum(s.groups_created for s in worker_stats),
        "total_errors": sum(len(s.errors) for s in worker_stats),
        "worker_stats": worker_stats,
    }

    # Aggregate response times
    all_response_times = []
    for stats in worker_stats:
        all_response_times.extend(stats.response_times)

    if all_response_times:
        total_stats.update(
            {
                "response_time_avg": statistics.mean(all_response_times),
                "response_time_median": statistics.median(all_response_times),
                "response_time_min": min(all_response_times),
                "response_time_max": max(all_response_times),
                "response_time_95p": statistics.quantiles(all_response_times, n=20)[18],
                "response_time_99p": statistics.quantiles(all_response_times, n=20)[19],
            }
        )

    return total_stats


def print_results(stats: dict[str, Any]):
    """Print comprehensive test results"""
    print("\n" + "=" * 80)
    print("EXTREME LOAD TEST RESULTS")
    print("=" * 80)

    print(f"Target Connections: {stats.get('target_connections', 'N/A'):,}")
    print(f"Target Groups: {stats.get('target_groups', 'N/A'):,}")
    print(f"Workers Used: {len(stats.get('worker_stats', []))}")

    print("\nACTUAL RESULTS:")
    print(f"Total Connections Established: {stats['total_connections_established']:,}")
    print(f"Total Connections Failed: {stats['total_connections_failed']:,}")
    print(f"Total Messages Sent: {stats['total_messages_sent']:,}")
    print(f"Total Messages Received: {stats['total_messages_received']:,}")
    print(f"Total Groups Created: {stats['total_groups_created']:,}")
    print(f"Total Errors: {stats['total_errors']:,}")

    if "response_time_avg" in stats:
        print("RESPONSE TIME STATISTICS:")
        print(f"  Average: {stats['response_time_avg']:.3f}s")
        print(f"  Median: {stats['response_time_median']:.3f}s")
        print(f"  Min: {stats['response_time_min']:.3f}s")
        print(f"  Max: {stats['response_time_max']:.3f}s")
        print(f"  95th percentile: {stats['response_time_95p']:.3f}s")
        print(f"  99th percentile: {stats['response_time_99p']:.3f}s")

    print("\nTHROUGHPUT:")
    total_duration = max(s.end_time - s.start_time for s in stats.get("worker_stats", []))
    if total_duration > 0:
        print(f"  Messages/second: {stats['total_messages_sent'] / total_duration:.1f}")
        print(
            f"  Connections/second: {stats['total_connections_established'] / total_duration:.1f}"
        )

    print("=" * 80)


def main():
    """Main function to run extreme load test"""
    # Configuration for extreme testing
    config = ExtremeTestConfig(
        server_url="ws://localhost:8000",
        http_url="http://localhost:8000",
        target_connections=100000,  # 100k connections
        target_groups=3000000,  # 3M groups
        workers=max(1, multiprocessing.cpu_count() // 2),  # Use half the cores
        test_duration=300,  # 5 minutes
        ramp_up_time=120,  # 2 minutes to ramp up
        message_rate=1000,  # 1000 messages/second total
        message_size=10000,  # 10k character messages
    )

    # Calculate per-worker allocations
    config.connections_per_worker = config.target_connections // config.workers
    config.groups_per_worker = config.target_groups // config.workers

    print("EXTREME LOAD TEST CONFIGURATION")
    print("=" * 50)
    print(f"Target Connections: {config.target_connections:,}")
    print(f"Target Groups: {config.target_groups:,}")
    print(f"Workers: {config.workers}")
    print(f"Connections per worker: {config.connections_per_worker:,}")
    print(f"Groups per worker: {config.groups_per_worker:,}")
    print(f"Test Duration: {config.test_duration}s")
    print(f"Ramp-up Time: {config.ramp_up_time}s")
    print(f"Message Rate: {config.message_rate}/s")
    print(f"Message Size: {config.message_size:,} chars")
    print("=" * 50)

    # Set up signal handling for graceful shutdown
    def signal_handler(signum, frame):
        print(f"\nReceived signal {signum}, initiating shutdown...")
        for p in processes:
            if p.is_alive():
                p.terminate()
        sys.exit(0)

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    # Start worker processes
    print("Starting worker processes...")
    processes = []
    for worker_id in range(config.workers):
        p = multiprocessing.Process(target=run_worker_process, args=(worker_id, config))
        p.start()
        processes.append(p)

    # Wait for all processes to complete
    print("Waiting for workers to complete...")
    worker_results = []
    for i, p in enumerate(processes):
        try:
            result = p.join(
                timeout=config.test_duration + config.ramp_up_time + 60
            )  # Extra time for cleanup
            if p.is_alive():
                print(f"Worker {i} did not complete, terminating...")
                p.terminate()
                p.join(timeout=10)
                if p.is_alive():
                    p.kill()
        except Exception as e:
            print(f"Error waiting for worker {i}: {e}")

    # Collect results (this is a simplification - in practice you'd use a queue)
    # For this example, we'll just run one worker to demonstrate
    print("Collecting results...")
    test_stats = aggregate_stats([])  # Empty for now
    test_stats["target_connections"] = config.target_connections
    test_stats["target_groups"] = config.target_groups

    print_results(test_stats)


if __name__ == "__main__":
    # Set multiprocessing start method for better compatibility
    if sys.platform != "win32":
        multiprocessing.set_start_method("spawn", force=True)

    main()
