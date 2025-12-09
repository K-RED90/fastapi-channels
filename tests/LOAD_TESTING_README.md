# Load Testing Scripts

This directory contains scripts for load testing the WebSocket chat system with high concurrent connections and large group counts.

## Scripts Overview

### `load_test.py`
**Purpose**: Realistic load testing with thousands of connections and groups
- **Connections**: Up to 5,000 concurrent WebSocket connections
- **Groups**: Up to 50,000 groups/rooms
- **Messages**: 500 messages/second with 5k character payloads
- **Duration**: 2 minutes of sustained load

**Use Case**: Production-like testing on a single powerful machine

### `stress_test.py`
**Purpose**: Moderate stress testing with configurable parameters
- **Connections**: Configurable (default 5,000)
- **Groups**: Configurable (default 50,000)
- **Messages**: Configurable rate and size
- **Features**: WebSocket + HTTP API testing

**Use Case**: Flexible testing with custom parameters

### `extreme_load_test.py`
**Purpose**: Extreme load testing designed for 100k+ connections and 3M+ groups
- **Architecture**: Multi-process distributed testing
- **Connections**: 100,000+ concurrent connections (distributed across workers)
- **Groups**: 3,000,000+ groups
- **Features**: Multi-worker architecture, gradual ramp-up, heartbeat maintenance

**Use Case**: Maximum scale testing on clusters or very powerful hardware

## Prerequisites

### System Requirements

For `load_test.py` (realistic testing):
- **RAM**: 16GB+ recommended
- **CPU**: 8+ cores recommended
- **Network**: Stable high-bandwidth connection

For `extreme_load_test.py` (maximum scale):
- **RAM**: 64GB+ recommended
- **CPU**: 16+ cores recommended
- **Storage**: Fast SSD storage
- **Network**: Multiple network interfaces recommended

### Server Configuration

Before running load tests, update the server configuration to handle high loads:

```python
# In core/config.py or environment variables
MAX_TOTAL_CONNECTIONS=200000
MAX_CONNECTIONS_PER_CLIENT=1000
MAX_TOTAL_GROUPS=5000000
WS_MAX_MESSAGE_SIZE=10485760  # 10MB
```

### Python Dependencies

```bash
pip install websockets aiohttp
```

## Running the Tests

### 1. Start the Server

First, start your WebSocket chat server:

```bash
# Make sure the server is running on localhost:8000
python -m uvicorn example.main:app --host 0.0.0.0 --port 8000
```

### 2. Run Load Tests

#### Basic Load Test (Recommended)
```bash
python load_test.py
```

#### Custom Stress Test
```bash
# Edit stress_test.py to modify LoadTestConfig parameters
python stress_test.py
```

#### Extreme Load Test (Advanced)
```bash
# Requires powerful hardware and careful monitoring
python extreme_load_test.py
```

## Test Results

Each script outputs comprehensive statistics:

### Connection Metrics
- Connections established vs failed
- Connection success rate
- Ramp-up performance

### Group Metrics
- Groups created vs failed
- Group creation throughput
- API response times

### Message Metrics
- Messages sent/received per second
- Message success rate
- Response time percentiles (median, 95th, 99th)

### System Health
- Throughput history over time
- Error samples
- Resource utilization insights

## Scaling Considerations

### Vertical Scaling (Single Machine)
- Increase RAM for more connections
- Use faster CPU for better throughput
- Optimize network stack (TCP tuning)

### Horizontal Scaling (Multiple Machines)
- Use `extreme_load_test.py` with multiple workers
- Distribute load across server instances
- Monitor inter-server communication

### Server Optimization
- **Redis Backend**: Required for high concurrency
- **Connection Pooling**: Configure Redis connection pools
- **Message Batching**: Implement message batching for broadcasts
- **Rate Limiting**: Tune rate limits for load testing

## Troubleshooting

### Common Issues

#### Connection Failures
- **Cause**: Server connection limits or resource exhaustion
- **Solution**: Increase server limits, add more RAM, check ulimits

#### Memory Issues
- **Cause**: Too many concurrent connections
- **Solution**: Reduce `max_connections`, increase system RAM

#### Slow Group Creation
- **Cause**: HTTP API bottlenecks
- **Solution**: Use batch operations, optimize database queries

#### Message Timeouts
- **Cause**: Server overload or network latency
- **Solution**: Reduce message rate, check network configuration

### Monitoring During Tests

Monitor these metrics during load testing:

```bash
# System resources
htop
iotop
nload

# Application metrics (if available)
# Check server logs for performance data
tail -f server.log
```

### Optimizing Performance

1. **Database**: Use Redis for session storage
2. **Connection Pooling**: Configure appropriate pool sizes
3. **Message Compression**: Enable WebSocket compression
4. **Load Balancing**: Distribute load across multiple server instances

## Expected Results

### Realistic Load Test (`load_test.py`)
- **Connections**: 3,000-5,000 concurrent
- **Groups**: 40,000-50,000 created
- **Throughput**: 300-500 messages/second
- **Response Time**: <100ms median

### Extreme Load Test (`extreme_load_test.py`)
- **Connections**: 50,000+ concurrent (with proper hardware)
- **Groups**: 1,000,000+ created
- **Throughput**: 1,000+ messages/second
- **Response Time**: <500ms median (under load)

## Safety Precautions

1. **Resource Limits**: Set appropriate system ulimits
2. **Monitoring**: Always monitor system resources
3. **Gradual Ramp-up**: Use gradual connection ramp-up
4. **Circuit Breakers**: Implement test timeouts and failure handling
5. **Cleanup**: Ensure proper connection cleanup after tests

## Interpreting Results

### Success Criteria
- **Connection Success Rate**: >95%
- **Message Success Rate**: >90%
- **Response Time P95**: <500ms
- **No Critical Errors**: Memory exhaustion, crashes

### Performance Benchmarks
- **Small Scale**: <1k connections, basic functionality
- **Medium Scale**: 1k-10k connections, moderate load
- **Large Scale**: 10k-100k connections, high load
- **Extreme Scale**: 100k+ connections, maximum load

This testing framework allows you to validate that your WebSocket chat system can handle the extreme loads you specified: 100,000+ concurrent connections and 3,000,000+ groups.