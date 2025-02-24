# Makefile for building and testing declafka

# Default target
.PHONY: all
all: test

# Build the Docker image
.PHONY: build
build:
	cargo build --workspace

# Run tests
.PHONY: test
test: build
	cargo test --workspace --quiet

# Clean up
.PHONY: clean
clean:
	cargo clean

# Run the example app
.PHONY: run
run: build
	docker-compose up -d
	cargo run -p example_app

# Stop the example app
.PHONY: stop
stop:
	docker-compose down
