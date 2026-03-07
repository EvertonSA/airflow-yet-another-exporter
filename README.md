# Airflow Yet Another Exporter (Airflow 3)

This project is a custom Prometheus/OpenTelemetry exporter built for **Apache Airflow 3**. It directly connects to the Airflow PostgreSQL database to scrape relevant metrics and exposes them via a web server.

## Prerequisites

- [Go](https://go.dev/doc/install) 1.21+
- [Docker](https://docs.docker.com/get-docker/) & [Docker Compose](https://docs.docker.com/compose/install/) (for local testing environment)

## Quick Start (Docker Compose)

The easiest way to run the exporter along with a local Airflow 3 and PostgreSQL environment is using Docker Compose:

```bash
# Start the entire stack in the background
make up

# Stop the stack
make down
```

## Running Locally (Development)

To run the exporter locally, you need to provide a connection string to the Airflow database.

### 1. Download Dependencies

```bash
make deps
```

### 2. Set Configuration Variables

The exporter uses environment variables prefixed with `AIRFLOW_EXPORTER_`. The only strictly required variable is the database connection string.

```bash
export AIRFLOW_EXPORTER_DATABASE_CONNECTION_STRING="postgres://airflow:airflow@localhost:5432/airflow?sslmode=disable"

# Optional environment variables
export AIRFLOW_EXPORTER_SERVER_PORT="8080"            # Port for the web server (Default: 8080)
export AIRFLOW_EXPORTER_LOG_LEVEL="info"              # Log level (Default: info)
export AIRFLOW_EXPORTER_OTEL_ENDPOINT="localhost:4317" # OpenTelemetry collector endpoint (Default: localhost:4317)
export AIRFLOW_EXPORTER_SCRAPE_INTERVAL="30s"         # Metric scrape interval (Default: 30s)
```

### 3. Run the Exporter

You can run the application directly:

```bash
make run
```
Or build the binary and run it manually:

```bash
make build
./bin/airflow-exporter serve
```

## Building the Docker Image

You can build the Docker image for the exporter independently:

```bash
make docker-build
```

## Testing and Linting

```bash
# Run unit tests with the race detector
make test

# Run Go linters (requires golangci-lint)
make lint
```