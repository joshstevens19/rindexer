release:
	./release.sh
prod_build:
	RUSTFLAGS='-C target-cpu=native' cargo build --release --features jemalloc