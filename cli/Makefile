new_no_code:
	cargo run -- new --path /Users/joshstevens/code/rindexer/example no-code
start:
	cargo run -- start --path /Users/joshstevens/code/rindexer/examples/rindexer_demo_cli all
start_prod:
	RUSTFLAGS='-C target-cpu=native' cargo run --profile production --features jemalloc -- start all
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
	cargo run -- codegen all
codegen_typings:
	cargo run -- codegen typings
codegen_indexer:
	cargo run -- codegen indexer
codegen_graphql:
	cargo run -- codegen --path /Users/joshstevens/code/rindexer/examples/base_paint graphql --endpoint  http://0.0.0.0:5005/graphql
download_abi:
	cargo run -- download-abi --path /Users/joshstevens/code/rindexer/examples/rindexer_demo_cli
delete:
	cargo run -- delete --path /Users/joshstevens/code/rindexer/examples/rindexer_demo_cli
prod_build:
	RUSTFLAGS='-C target-cpu=native' cargo build --release --features jemalloc