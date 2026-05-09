# ── Stage 1: Builder ──────────────────────────────────────────────────────────
# golang:1.22 (Debian Bookworm) включает git, ca-certificates и tzdata.
# Никаких вызовов пакетного менеджера не нужно — Alpine CDN не задействован.
FROM golang:1.22 AS builder

WORKDIR /app
COPY . .

# go mod tidy генерирует go.sum если его нет, затем собираем статический бинарь.
RUN go mod tidy && \
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -ldflags="-s -w" -o /out/event-processor ./cmd/main.go

# ── Stage 2: Runtime ──────────────────────────────────────────────────────────
# debian:bookworm-slim надёжнее scratch для production-like окружения:
#   - нет edge-кейсов с DNS, /etc/hosts, /etc/nsswitch.conf
#   - wget доступен для Docker healthcheck
#   - ca-certificates и tzdata устанавливаются через apt (debian CDN стабильнее Alpine)
FROM debian:bookworm-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates \
        tzdata \
        wget \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd -r app && useradd -r -g app -d /app -s /usr/sbin/nologin app

WORKDIR /app
COPY --from=builder /out/event-processor /app/event-processor

USER app

EXPOSE 8080

ENTRYPOINT ["/app/event-processor"]
