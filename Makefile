# Makefile for building and testing KafkaTestContainer

# Variables
IMAGE_NAME = jamfor352/kafka-test-container
DOCKERFILE = KafkaTestContainer.dockerfile

# Default target
.PHONY: all
all: test

# Build the Docker image
.PHONY: build
build:
	docker build -t $(IMAGE_NAME) -f $(DOCKERFILE) .

# Run tests
.PHONY: test
test: build
	cargo test --workspace --quiet

# Clean up
.PHONY: clean
clean:
	docker rmi $(IMAGE_NAME) || true
