# Rindexer E2E Tests

End-to-end tests for the Rindexer high-performance EVM event indexer, built with a registry-based test system inspired by the `rrelayer` project.

## 🚀 Quick Start

### Prerequisites

- Rust (latest stable)
- Foundry (for Anvil blockchain)
- Rindexer binary (will be built automatically)

### Setup

```bash
# Complete development setup
make dev-setup

# Or step by step:
make install-deps    # Install Foundry and Anvil
make build-rindexer  # Build Rindexer binary
make build          # Build E2E test binary
```

### Running Tests

```bash
# Run all tests (recommended)
make run-tests

# Run with debug output
make run-tests-debug

# Run only historical tests
make run-tests-historical

# Run only live indexing tests
make run-tests-live
```

## 🧪 Test Categories

### Historical Indexing Tests
- **`test_1_basic_connection`**: Basic Rindexer connection to Anvil
- **`test_2_contract_discovery`**: Contract ABI discovery and event registration
- **`test_3_historic_indexing`**: Historic event indexing from contract deployment
- **`test_6_demo_yaml`**: Demo YAML configuration test
- **`test_restart_checkpoint_no_duplicates`**: Restart and ensure no duplicates

### Live Indexing Tests
- **`test_live_indexing_basic`**: Live indexing with background transaction feeder
- **`test_live_indexing_high_frequency`**: High-frequency live indexing test

### GraphQL Tests
- **`test_graphql_service_starts`**: Start all services with Postgres and keep GraphQL up
- **`test_graphql_basic_query`**: Query transfers with filter & pagination

### Postgres E2E Tests
- **`test_postgres_end_to_end`**: Enable Postgres, index, and verify rows inserted
- **`test_postgres_live_exact_events`**: Feed live transfers and assert exact recipients

### Direct RPC Test
- **`test_direct_rpc`**: Rocket Pool rETH Transfer vs expected CSV using a mainnet RPC

### Multi-Network Test
- **`test_multi_network_mixed`**: Mainnet rETH (historic) + Anvil SimpleERC20 (historic)

### Config Validation
- **`test_invalid_yaml_fails`**: Invalid YAML fails fast with actionable error
- **`test_missing_abi_path_fails`**: Missing ABI path yields actionable error

## 🎯 Individual Test Execution

```bash
# Run a specific test
make run-test TEST=test_1_basic_connection

# Run with debug output
make run-test-debug TEST=test_live_indexing_basic

# Convenience shortcuts
make test-basic        # Basic connection test
make test-contract     # Contract discovery test
make test-historic     # Historic indexing test
make test-live-basic   # Basic live indexing test
```

## 🔧 Development

### Adding New Tests

1. Create a new module in `src/tests/` (e.g., `my_new_test.rs`)
2. Implement the `TestModule` trait:

```rust
use crate::test_suite::TestContext;
use crate::tests::registry::{TestDefinition, TestModule};

pub struct MyNewTests;

impl TestModule for MyNewTests {
    fn get_tests() -> Vec<TestDefinition> {
        vec![
            TestDefinition::new(
                "test_my_new_feature",
                "Test my new feature",
                my_new_test_function,
            ).with_timeout(120),
        ]
    }
}

fn my_new_test_function(context: &mut TestContext) -> Pin<Box<dyn Future<Output = Result<()>> + '_>> {
    Box::pin(async move {
        // Your test logic here
        Ok(())
    })
}
```

3. Register the module in `src/tests/registry.rs`:

```rust
impl TestRegistry {
    pub fn get_all_tests() -> Vec<TestDefinition> {
        let mut tests = Vec::new();
        // ... existing tests ...
        tests.extend(crate::tests::my_new_test::MyNewTests::get_tests());
        tests
    }
}
```

### Live Indexing Tests

For tests that need background transaction generation, mark them as live tests:

```rust
TestDefinition::new(
    "test_live_my_feature",
    "Test my live feature",
    my_live_test_function,
).with_timeout(120).as_live_test()
```

The `TestRunner` will automatically start a `LiveFeeder` for these tests.

#### Chain ID Handling

Live transactions are signed against the node's actual chain ID (queried at runtime). If you ever see a "DifferentChainID" error, ensure your local node is reachable and not overridden by a different chain configuration.

## 🔧 Paths: Contracts and ABIs

- Test contracts live under `e2e-tests/contracts/` and are deployed with Foundry `forge`.
- ABI files live under `e2e-tests/abis/`. The runner copies them into the ephemeral project folder under `./abis/` before launching Rindexer.
- Per-test temporary projects are created in your system temp directory and cleaned up after execution.

## 🔑 Environment Variables

Some tests require external RPCs or accept optional tuning:

- `MAINNET_RPC_URL` (required for multi-network tests): HTTPS RPC for Ethereum mainnet.
- `DIRECT_RPC_EXPECTED_CSV` (optional): Path to expected CSV for direct RPC comparisons. Defaults to `data/rocketpooleth-transfer.csv`.
- `MULTI_NETWORK_SYNC_TIMEOUT` (optional): Seconds to wait for multi-network historic sync. Defaults to `600`.

## 🏗️ Architecture

### Registry System
- **`TestRegistry`**: Central test discovery and management
- **`TestDefinition`**: Individual test metadata and execution
- **`TestRunner`**: Orchestrates test execution with timeouts and reporting

### Infrastructure
- **`AnvilManager`**: Manages local Anvil blockchain instances
- **`LiveFeeder`**: Background transaction submission and mining
- **`TestContext`**: Shared test utilities and state management

### Test Execution Flow
```
main.rs → run_tests() → TestRunner → TestRegistry → Individual Test Functions
```

## 📊 Test Results

The system provides detailed test reporting:

```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
[SUCCESS] Test Suites: 1 passed, 1 total
[SUCCESS] Tests:       7 passed, 7 total
[TIME] Time:        45.23s
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🎉 All tests passed!
```

## 🐛 Debugging

### Logs
```bash
make logs          # Show recent logs
make logs-live     # Follow live logs
make logs-clear    # Clear all logs
```

### Service Status
```bash
make check-services  # Check if Anvil is running
```

### Debug Mode
```bash
# Run with debug logging
make run-tests-debug

# Run single test with debug
make run-test-debug TEST=test_1_basic_connection
```

## 🚀 CI/CD Integration

```bash
# CI-friendly targets
make ci-test              # Run all tests
make ci-test-historical   # Run only historical tests
make ci-test-live         # Run only live tests
```

## 📁 File Organization

```
src/
├── main.rs                 # CLI entry point
├── lib.rs                  # Library exports
├── anvil_setup.rs          # Anvil blockchain management
├── rindexer_client.rs      # Rindexer process management
├── health_client.rs        # Health check client
├── test_suite.rs           # Test context & utilities
├── live_feeder.rs          # Background transaction feeder
└── tests/
    ├── mod.rs              # Main test module exports
    ├── registry.rs         # Central test registry
    ├── test_runner.rs      # Test execution orchestrator
    ├── test_suite.rs       # Test results & reporting
    ├── basic_connection.rs # Basic connection test
    ├── contract_discovery.rs # Contract discovery test
    ├── historic_indexing.rs # Historic indexing test
    ├── demo_yaml.rs        # Demo YAML test
    ├── forked_anvil.rs     # Forked Anvil test
    └── live_indexing.rs    # Live indexing tests
```

## 🤝 Contributing

1. Follow the existing test patterns
2. Add appropriate timeouts for your tests
3. Use `LiveFeeder` for tests that need background transactions
4. Update this README when adding new test categories
5. Ensure all tests pass with `make run-tests`