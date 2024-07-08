new_no_code:
	cargo run -- new --path /Users/joshstevens/code/rindexer/examples no-code
new_rust:
	RUSTFLAGS='-C target-cpu=native' cargo run --release --features jemalloc -- new --path /Users/joshstevens/code rust
start:
	cargo run -- start --path /Users/joshstevens/code/rindexer/examples/rindexer_demo_cli all
start_prod:
	RUSTFLAGS='-C target-cpu=native' cargo run --release --features jemalloc -- start all
start_indexer:
	cargo run -- start --path /Users/joshstevens/code/rindexer/examples/rindexer_demo_cli indexer
start_indexer_base_paint:
	RUSTFLAGS='-C target-cpu=native' cargo run --release --features jemalloc -- start --path /Users/joshstevens/code/rindexer/examples/base_paint indexer
start_graphql_base_paint:
	RUSTFLAGS='-C target-cpu=native' cargo run --release --features jemalloc -- start --path /Users/joshstevens/code/rindexer/examples/base_paint graphql
start_indexer_prod:
	RUSTFLAGS='-C target-cpu=native' cargo run --release --features jemalloc -- start --path /Users/joshstevens/code/rindexer/examples/rindexer_demo_cli all
start_indexer_lens_mirrors:
	RUSTFLAGS='-C target-cpu=native' cargo run --release --features jemalloc -- start --path /Users/joshstevens/code/rindexer/examples/lens_mirrors indexer
start_graphql:
	cargo run -- start --path /Users/joshstevens/code/rindexer/examples/rindexer_demo_cli graphql
codegen:
	RUSTFLAGS='-C target-cpu=native' cargo run --release --features jemalloc -- codegen --path /Users/joshstevens/code/rust typings
codegen_typings:
	cargo run -- codegen typings
codegen_indexer:
	cargo run -- codegen indexer
codegen_graphql:
	cargo run -- codegen --path /Users/joshstevens/code/rindexer/examples/base_paint graphql --endpoint  http://0.0.0.0:5005/graphql
add_contract:
	RUSTFLAGS='-C target-cpu=native' cargo run --release --features jemalloc -- add --path /Users/joshstevens/code/rindexer/examples/rindexer_demo_cli contract
delete:
	cargo run -- delete --path /Users/joshstevens/code/rindexer/examples/rindexer_demo_cli
prod_build:
	RUSTFLAGS='-C target-cpu=native' cargo build --release --features jemalloc