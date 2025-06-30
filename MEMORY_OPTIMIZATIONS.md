# Memory Optimizations and Leak Fixes

This document outlines the memory optimizations and leak fixes implemented in the rindexer codebase.

## Critical Memory Leaks Fixed

### 1. **INFINITE SHUTDOWN LOOP (70GB SPIKE)** ⚠️

**Problem**: During shutdown, tasks were hanging indefinitely due to database/file I/O operations, causing an infinite loop with 1 active task that never completed, leading to a 70GB+ memory spike.

**Root Cause**: 
- The `update_progress_and_last_synced_task` function was making blocking database and file I/O operations
- These operations could hang during shutdown when database connections were slow/unresponsive
- No timeout protection on these operations
- The shutdown process had no maximum timeout, leading to infinite loops

**Fix**: 
- Added 5-second timeouts to `update_progress_and_last_synced_task` and `evm_trace_update_progress_and_last_synced_task`
- Added 10-second maximum timeout to the shutdown process
- Added early completion logic when timeouts occur or shutdown is detected
- Added debug logging to task tracker to help identify hanging tasks
- Force shutdown completion even if some tasks are still pending

**Location**: `core/src/indexer/last_synced.rs`, `core/src/system_state.rs`, `core/src/indexer/task_tracker.rs`

### 2. **SHUTDOWN MEMORY LEAK (120GB SPIKE)** ⚠️

**Problem**: During shutdown, the `update_progress_and_last_synced_task` function was spawning new tasks for every event processing, leading to unbounded task accumulation and a massive memory spike (120GB+).

**Root Cause**: 
- Tasks were spawned during shutdown without checking if the system was shutting down
- No timeout on the shutdown process could lead to infinite loops
- Each event processing spawned a new task for database/CSV updates

**Fix**: 
- Added shutdown checks before spawning new tasks in `update_progress_and_last_synced_task`
- Added shutdown checks in `evm_trace_update_progress_and_last_synced_task`
- Added timeout protection to prevent infinite shutdown loops
- Direct completion callback execution during shutdown instead of spawning tasks

**Location**: `core/src/indexer/last_synced.rs`, `core/src/system_state.rs`

### 3. **MAX_BLOCK_RANGE CONFIGURATION IGNORED** ⚠️

**Problem**: The `max_block_range` setting from the YAML configuration was being ignored when RPC providers returned error messages with suggested block ranges, leading to larger-than-configured block range requests and potential timeouts.

**Root Cause**: 
- The `retry_with_block_range` function was overriding the YAML configuration with provider suggestions
- Provider error responses (Alchemy, Infura, etc.) were setting `max_block_range: None`, ignoring the original config
- No minimum comparison between original config and provider suggestions

**Fix**: 
- Modified `retry_with_block_range` to accept and respect the original `max_block_range` configuration
- Added minimum comparison logic to ensure provider suggestions don't exceed the configured limit
- Updated all provider-specific error handling to respect the original configuration
- Fixed function signatures to pass the original configuration through the call chain

**Location**: `core/src/indexer/fetch_logs.rs`

### 4. Unbounded Task Accumulation in `process_event_logs`

**Problem**: The `process_event_logs` function was spawning tasks but never awaiting them, leading to unbounded task accumulation and memory leaks.

**Fix**: 
- Added proper task tracking and awaiting
- Implemented conditional task awaiting based on `block_until_indexed` parameter
- Added cleanup of remaining tasks at the end of processing

**Location**: `core/src/indexer/process.rs:56-85`

### 5. Channel Size Memory Bounds

**Problem**: The channel size for log processing could grow unbounded, leading to memory exhaustion.

**Fix**:
- Added a maximum cap of 10 for the `RINDEXER_CHANNEL_SIZE` to prevent excessive memory usage
- Improved channel size validation and bounds checking

**Location**: `core/src/indexer/fetch_logs.rs:40-50`

### 6. Live Indexing Memory Pressure

**Problem**: Live indexing streams were accumulating memory without proper cleanup.

**Fix**:
- Added memory cleanup in live indexing streams
- Improved resource management for long-running streams
- Added proper cleanup of HashMap resources

**Location**: `core/src/indexer/fetch_logs.rs:250-260`, `core/src/indexer/process.rs:575-580`

## Memory Optimizations

### 1. Efficient EventResult Creation

**Optimization**: Avoid creating unnecessary objects when logs are empty.

**Location**: `core/src/indexer/process.rs:529-545`

### 2. Concurrent Task Limiting

**Optimization**: Added semaphore-based limiting to prevent memory exhaustion from too many concurrent tasks.

**Location**: `core/src/indexer/start.rs:280-300`

### 3. HashMap Pre-allocation

**Optimization**: Pre-allocate HashMap capacity to reduce reallocation overhead.

**Location**: `core/src/indexer/process.rs:200-210`

### 4. Memory Cleanup in Live Indexing

**Optimization**: Clear resources and references to reduce memory pressure during live indexing.

**Location**: `core/src/indexer/fetch_logs.rs:250-260`

## Configuration Improvements

### Environment Variables

- `RINDEXER_CHANNEL_SIZE`: Maximum channel size (capped at 10)
- `RINDEXER_MAX_CONCURRENT_TASKS`: Maximum concurrent tasks (capped at 100)

### YAML Configuration

- `max_block_range`: Now properly respected across all RPC providers
- Improved error handling to maintain configuration limits

## Performance Impact

These optimizations should result in:
- **Significantly reduced memory usage** during normal operation
- **Elimination of the 70GB and 120GB shutdown memory spikes**
- **Proper respect for YAML configuration limits**
- **Better resource management** and cleanup
- **Improved stability** during high-load scenarios
- **Fast and reliable shutdown** with timeout protection

## Testing Recommendations

1. **Memory Monitoring**: Monitor memory usage during startup, operation, and shutdown
2. **Configuration Testing**: Verify that `max_block_range` settings are properly respected
3. **Load Testing**: Test with high-volume indexing scenarios
4. **Shutdown Testing**: Verify clean shutdown without memory spikes or infinite loops
5. **Timeout Testing**: Verify that shutdown completes within 10 seconds even with hanging tasks

## Environment Variables for Memory Control

1. **Set `RINDEXER_CHANNEL_SIZE`** to limit channel buffer sizes (max 10)
2. **Set `RINDEXER_MAX_CONCURRENT_TASKS`** to limit concurrent processing (max 100)
3. **Monitor task counts** during operation and shutdown
4. **Check shutdown state** before spawning new tasks
5. **Add timeouts** to prevent infinite loops
6. **Force completion** of hanging tasks during shutdown

The trade-offs are designed to prioritize system stability over maximum performance.

## Critical Fix for Production

The shutdown memory leak fix is **critical for production deployments**. Without this fix, shutdowns can cause:
- Memory spikes of 100GB+ 
- System OOM kills
- Unreliable shutdown behavior
- Resource exhaustion

This fix should be deployed immediately in any production environment. 