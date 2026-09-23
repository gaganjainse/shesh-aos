.PHONY: help check test lint fmt build bench clean

help:
	@echo "SheshaAOS Development Commands"
	@echo ""
	@echo "  make check    - Run cargo check"
	@echo "  make test     - Run all tests"
	@echo "  make lint     - Run fmt check and clippy"
	@echo "  make fmt      - Format code"
	@echo "  make build    - Build workspace"
	@echo "  make bench    - Run benchmarks"
	@echo "  make clean    - Clean build artifacts"
	@echo "  make all      - Run fmt, lint, test, build"

check:
	$(MAKE) lint
	$(MAKE) test
	cargo check --workspace

test:
	cargo test --workspace

lint:
	cargo fmt --check
	cargo clippy --workspace --all-targets -- -D warnings

fmt:
	cargo fmt

build:
	cargo build --workspace

bench:
	cargo bench --workspace

clean:
	cargo clean
	rm -rf target/

all: check build
	@echo "==> All checks passed!"
