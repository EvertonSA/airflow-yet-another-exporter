# Dockerfile
# Stage 1: Builder
FROM golang:1.25-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Build string
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /app/airflow-exporter ./cmd/airflow-exporter

# Stage 2: Runner
FROM gcr.io/distroless/static:nonroot

WORKDIR /

COPY --from=builder /app/airflow-exporter /airflow-exporter

USER 65532:65532

ENTRYPOINT ["/airflow-exporter"]
CMD ["serve"]
