# Makefile
.PHONY: all build run test lint docker-build help

BINARY_NAME=airflow-exporter
DOCKER_IMAGE=airflow-exporter:latest

all: lint test build

## Help: Show this help message
help:
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@awk '/^[a-zA-Z\-\_0-9]+:/ { \
		helpMessage = match(lastLine, /^## (.*)/); \
		if (helpMessage) { \
			helpCommand = substr($$1, 0, index($$1, ":")-1); \
			helpMessage = substr(lastLine, RSTART + 3, RLENGTH); \
			printf "  %-15s %s\n", helpCommand, helpMessage; \
		} \
	} \
	{ lastLine = $$0 }' $(MAKEFILE_LIST)

## Build: Build the binary
build:
	@echo "Building $(BINARY_NAME)..."
	go build -o bin/$(BINARY_NAME) ./cmd/airflow-exporter

## Run: Run the application locally
run:
	@echo "Running $(BINARY_NAME)..."
	go run ./cmd/airflow-exporter serve

## Test: Run unit tests
test:
	@echo "Running tests..."
	go test -v -race ./...

## Lint: Run linters
lint:
	@echo "Running linters..."
	go run github.com/golangci/golangci-lint/cmd/golangci-lint@v1.61.0 run ./...

## Docker Build: Build the docker image
docker-build:
	@echo "Building Docker image $(DOCKER_IMAGE)..."
	docker build -t $(DOCKER_IMAGE) .

## Up: Start local environment (Airflow + Postgres + Exporter)
up:
	@echo "Starting local environment..."
	docker-compose up --build -d

## Down: Stop local environment
down:
	@echo "Stopping local environment..."
	docker-compose down


## Deps: Download dependencies
deps:
	@echo "Downloading dependencies..."
	go mod download
	go mod tidy
