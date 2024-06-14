new:
	cargo run -- new
start:
	cargo run -- start all
start_prod:
	RUSTFLAGS='-C target-cpu=native' cargo run --profile production --features jemalloc -- start all
start_indexer:
	cargo run -- start indexer
start_graphql:
	cargo run -- start graphql
codegen:
	cargo run -- codegen all
codegen_typings:
	cargo run -- codegen typings
codegen_indexer:
	cargo run -- codegen indexer
download_abi:
	cargo run -- download_abi