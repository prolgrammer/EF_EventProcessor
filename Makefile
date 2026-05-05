.PHONY: build docker-build up down logs restart test lint tidy kafka-topics seed help

BINARY   := event-processor
IMAGE    := eventflow/event-processor
VERSION  := $(shell git rev-parse --short HEAD 2>/dev/null || echo dev)

# ── Build ─────────────────────────────────────────────────────────────────────

build:
	@echo ">>> Building binary..."
	CGO_ENABLED=0 go build -ldflags="-s -w" -o bin/$(BINARY) ./cmd/main.go

docker-build:
	@echo ">>> Building Docker image $(IMAGE):$(VERSION)..."
	docker build -t $(IMAGE):$(VERSION) -t $(IMAGE):latest .

# ── Docker Compose ────────────────────────────────────────────────────────────

up:
	@echo ">>> Starting all services..."
	docker compose up -d --build

down:
	@echo ">>> Stopping all services..."
	docker compose down

logs:
	docker compose logs -f event-processor

restart:
	docker compose restart event-processor

# ── Kafka ─────────────────────────────────────────────────────────────────────

kafka-topics:
	@echo ">>> Creating Kafka topics..."
	docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
		--bootstrap-server localhost:9092 \
		--create --if-not-exists --topic events.raw   --partitions 6 --replication-factor 1
	docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
		--bootstrap-server localhost:9092 \
		--create --if-not-exists --topic events.valid --partitions 6 --replication-factor 1
	docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
		--bootstrap-server localhost:9092 \
		--create --if-not-exists --topic events.dlq   --partitions 3 --replication-factor 1
	@echo ">>> Current topics:"
	docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

# ── Testing ───────────────────────────────────────────────────────────────────

test:
	@echo ">>> Running Go unit tests..."
	go test -race -count=1 ./...

test-python:
	@echo ">>> Running Python integration tests (зависимости ставятся автоматически)..."
	python3 tests/test_service.py

# ── Quality ───────────────────────────────────────────────────────────────────

lint:
	golangci-lint run ./...

tidy:
	go mod tidy

# ── Seed data ─────────────────────────────────────────────────────────────────

seed:
	@echo ">>> Sending 10 test events to Kafka..."
	@python3 tests/seed.py 2>/dev/null || echo "Run 'make test-python' to install dependencies first"

# ── Help ──────────────────────────────────────────────────────────────────────

help:
	@echo ""
	@echo "  make up           Start all services (Kafka, ClickHouse, event-processor, Prometheus, Grafana)"
	@echo "  make down         Stop all services"
	@echo "  make logs         Tail event-processor logs"
	@echo "  make restart      Rebuild and restart event-processor"
	@echo "  make build        Build the Go binary locally"
	@echo "  make docker-build Build the Docker image"
	@echo "  make kafka-topics Create Kafka topics manually"
	@echo "  make test         Run Go unit tests"
	@echo "  make test-python  Run Python integration tests"
	@echo "  make lint         Run golangci-lint"
	@echo "  make tidy         Run go mod tidy"
	@echo "  make seed         Send test events to Kafka"
	@echo ""
