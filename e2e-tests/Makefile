# Rindexer E2E Tests Makefile

# Override the rindexer binary path
RINDEXER_BIN ?= ../target/release/rindexer_cli

.PHONY: help install-deps clean build \
        start-anvil stop-anvil \
        build-rindexer \
        test-full test-quick run-tests run-tests-debug \
        run-tests-historical run-tests-live run-tests-all \
        run-test run-test-debug \
        test-basic test-contract test-historic test-demo test-forked \
        test-live-basic test-live-high-freq \
        logs logs-live logs-anvil logs-clear check-services

help: ## Show this help message
	@echo "🚀 Rindexer E2E Tests"
	@echo "Available targets:"
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  \033[36m%-25s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

# =============================================================================
# SETUP & DEPENDENCIES
# =============================================================================

install-deps: ## Install required dependencies (Foundry, Anvil)
	@echo "Installing Foundry..."
	@curl -L https://foundry.paradigm.xyz | bash
	@foundryup
	@echo "Installing Anvil (if not already installed)..."
	@cargo install --git https://github.com/foundry-rs/foundry anvil chisel || echo "Anvil/chisel already installed or installation failed - continuing..."
	@echo "Checking Anvil installation..."
	@anvil --version || (echo "Anvil not found, trying alternative installation..." && cargo install anvil)

build: ## Build the E2E test binary
	@cargo build --bin e2e-tests

build-rindexer: ## Build the Rindexer binary using the CLI Makefile
	@echo "Building Rindexer binary using CLI Makefile..."
	@cd ../cli && make prod_build
	@echo "Rindexer binary built at ../target/release/rindexer_cli"

clean: stop-anvil ## Clean up all running processes and files
	@rm -f *.pid *.log
	@cargo clean
	@echo "Cleanup complete"

# =============================================================================
# ANVIL BLOCKCHAIN
# =============================================================================

start-anvil: ## Start Anvil blockchain
	@echo "Starting Anvil..."
	@anvil --host 0.0.0.0 --port 8545 --accounts 10 \
		--mnemonic "test test test test test test test test test test test junk" \
		--block-time 1 --gas-limit 30000000 > anvil.log 2>&1 &
	@echo $$! > anvil.pid
	@sleep 2
	@if [ -f anvil.pid ]; then \
		echo "Anvil started (PID: $$(cat anvil.pid))"; \
	else \
		echo "Anvil started (PID: unknown - managed by tests)"; \
	fi

stop-anvil: ## Stop Anvil blockchain
	@if [ -f anvil.pid ]; then \
		echo "Stopping Anvil (PID: $$(cat anvil.pid))..."; \
		kill $$(cat anvil.pid) 2>/dev/null || true; \
		rm -f anvil.pid anvil.log; \
	else \
		echo "Stopping Anvil (PID: unknown - managed by tests)..."; \
		pkill -f anvil 2>/dev/null || true; \
	fi

# =============================================================================
# TEST RUNNERS - MAIN TARGETS
# =============================================================================

run-tests: ## Run all E2E tests with clean output
	@echo "Running all E2E tests with clean output..."
	@RUST_LOG=error cargo run --bin e2e-tests -- --rindexer-binary $(RINDEXER_BIN)

run-tests-debug: ## Run all E2E tests with debug output
	@echo "Running all E2E tests with debug output..."
	@RUST_LOG=info cargo run --bin e2e-tests -- --rindexer-binary $(RINDEXER_BIN)

run-tests-historical: ## Run only historical indexing tests
	@echo "Running historical indexing tests..."
	@RUST_LOG=info cargo run --bin e2e-tests -- --rindexer-binary $(RINDEXER_BIN) --tests "test_1_basic_connection,test_2_contract_discovery,test_3_historic_indexing,test_6_demo_yaml,test_8_forked_anvil"

run-tests-live: ## Run only live indexing tests
	@echo "Running live indexing tests..."
	@RUST_LOG=info cargo run --bin e2e-tests -- --rindexer-binary $(RINDEXER_BIN) --tests "test_live_indexing_basic,test_live_indexing_high_frequency"

# =============================================================================
# INDIVIDUAL TEST RUNNERS
# =============================================================================

run-test: ## Run a single test (use TEST=test_name)
	@if [ -z "$(TEST)" ]; then \
		echo "❌ Error: TEST variable must be set. Usage: make run-test TEST=test_name"; \
		exit 1; \
	fi
	@echo "Running single test: $(TEST)..."
	@RUST_LOG=info cargo run --bin e2e-tests -- --rindexer-binary $(RINDEXER_BIN) --tests "$(TEST)"

run-test-debug: ## Run a single test with debug output (use TEST=test_name)
	@if [ -z "$(TEST)" ]; then \
		echo "❌ Error: TEST variable must be set. Usage: make run-test-debug TEST=test_name"; \
		exit 1; \
	fi
	@echo "Running single test: $(TEST) (debug output)..."
	@RUST_LOG=debug cargo run --bin e2e-tests -- --rindexer-binary $(RINDEXER_BIN) --tests "$(TEST)"

# =============================================================================
# CONVENIENCE TEST TARGETS
# =============================================================================

test-basic: ## Run basic connection test
	@make run-test TEST=test_1_basic_connection

test-contract: ## Run contract discovery test
	@make run-test TEST=test_2_contract_discovery

test-historic: ## Run historic indexing test
	@make run-test TEST=test_3_historic_indexing

test-demo: ## Run demo YAML test
	@make run-test TEST=test_6_demo_yaml

test-forked: ## Run forked Anvil test
	@make run-test TEST=test_8_forked_anvil

test-live-basic: ## Run basic live indexing test
	@make run-test TEST=test_live_indexing_basic

test-live-high-freq: ## Run high frequency live indexing test
	@make run-test TEST=test_live_indexing_high_frequency

# =============================================================================
# DEBUG AND DEVELOPMENT TARGETS
# =============================================================================

logs-clear: ## Clear all log files
	@rm -f *.log
	@echo "All log files cleared"

# =============================================================================
# DEVELOPMENT HELPERS
# =============================================================================

dev-setup: install-deps build-rindexer build ## Complete development setup
	@echo "✅ Development setup complete!"
	@echo "You can now run: make run-tests"

dev-test: build-rindexer run-tests-debug ## Quick development test cycle
	@echo "✅ Development test complete!"

# =============================================================================
# CI/CD TARGETS
# =============================================================================

ci-test: build-rindexer run-tests ## CI-friendly test run
	@echo "✅ CI test complete!"

ci-test-historical: build-rindexer run-tests-historical ## CI-friendly historical tests
	@echo "✅ CI historical test complete!"

ci-test-live: build-rindexer run-tests-live ## CI-friendly live tests
	@echo "✅ CI live test complete!"
