new:
	cargo run -- new --path /Users/joshstevens/code/rindexer/examples
start:
	cargo run -- start --path /Users/joshstevens/code/rindexer/examples/rindexer_demo_cli all
start_prod:
	RUSTFLAGS='-C target-cpu=native' cargo run --profile production --features jemalloc -- start all
start_indexer:
	cargo run -- start --path /Users/joshstevens/code/rindexer/examples/rindexer_demo_cli indexer
start_graphql:
	cargo run -- start --path /Users/joshstevens/code/rindexer/examples/rindexer_demo_cli graphql
codegen:
	cargo run -- codegen all
codegen_typings:
	cargo run -- codegen typings
codegen_indexer:
	cargo run -- codegen indexer
download_abi:
	cargo run -- download_abi --path /Users/joshstevens/code/rindexer/examples/rindexer_demo_cli
delete:
	cargo run -- delete --path /Users/joshstevens/code/rindexer/examples/rindexer_demo_cli