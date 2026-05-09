# event-processor

Микросервис платформы **EventFlow** — ETL-компонент write-path. Читает сырые события из Kafka, валидирует, обогащает и пакетно вставляет в ClickHouse; публикует обогащённые события в `events.valid` и невалидные в `events.dlq`.

---

## Архитектура сервиса

```
Kafka events.raw
       │
       ▼
 ┌─────────────┐
 │  Consumer   │  segmentio/kafka-go, consumer-group "processor"
 └──────┬──────┘
        │  []RawEvent (batch)
        ▼
 ┌─────────────┐
 │  Validator  │  проверка обязательных полей, временны́х ограничений
 └──────┬──────┘
        │  valid / DLQ split
        ▼
 ┌─────────────┐
 │  Enricher   │  UA-парсинг, GeoIP (stub → MaxMind), session stitching
 └──────┬──────┘
        │  []EnrichedEvent
        ▼
 ┌──────────────────────────────┐
 │  ClickHouse batch writer     │  INSERT каждые 1 с или 10 000 событий
 └──────────────────────────────┘
        │                    │
        ▼                    ▼
 Kafka events.valid    Kafka events.dlq
```

**Pipeline-паттерн** — каждая стадия работает в параллельных горутинах через `errgroup` с семафором (`VALIDATOR_WORKERS`). Kafka-оффсеты коммитятся **только после успешной записи в ClickHouse** → at-least-once семантика.

---

## Стек

| Компонент       | Технология                        |
|-----------------|-----------------------------------|
| Язык            | Go 1.22                           |
| Kafka client    | `segmentio/kafka-go`              |
| ClickHouse      | `ClickHouse/clickhouse-go/v2`     |
| HTTP-роутер     | `go-chi/chi/v5`                   |
| Метрики         | `prometheus/client_golang`        |
| Логирование     | `uber/zap` (JSON, stdout)         |
| Конфиг          | `kelseyhightower/envconfig`       |

---

## Быстрый старт

### Требования

- Docker ≥ 24 и Docker Compose plugin (`docker compose`)
- Python ≥ 3.10 (только для тестового скрипта)

### Запуск

```bash
# 1. Поднять всю инфраструктуру + сервис
docker compose up -d --build

# 2. Дождаться готовности (≈ 30–60 с)
docker compose ps          # все сервисы должны быть healthy / running

# 3. Проверить, что сервис живёт
curl http://localhost:8080/healthz
# {"status":"ok"}

curl http://localhost:8080/readyz
# {"status":"ready"}
```

### Отправить тестовые события

```bash
# Установить зависимости Python (один раз)
pip install -r tests/requirements.txt

# Отправить 10 событий в Kafka events.raw
python tests/seed.py

# Или через Make
make seed
```

### Запустить интеграционные тесты

```bash
make test-python
# или напрямую
python tests/test_service.py
```

### Просмотр метрик и дашбордов

| URL                                  | Что                        |
|--------------------------------------|----------------------------|
| http://localhost:8080/metrics        | Prometheus-метрики сервиса |
| http://localhost:9090                | Prometheus UI              |
| http://localhost:3000                | Grafana (admin / admin)    |
| http://localhost:8123/play           | ClickHouse HTTP playground |

---

## Переменные окружения

| Переменная                  | По умолчанию       | Описание                                              |
|-----------------------------|--------------------|-------------------------------------------------------|
| `KAFKA_BROKERS`             | `localhost:9092`   | Адреса брокеров (через запятую)                       |
| `KAFKA_GROUP_ID`            | `processor`        | Consumer-group ID                                     |
| `KAFKA_TOPIC_RAW`           | `events.raw`       | Входной топик сырых событий                           |
| `KAFKA_TOPIC_VALID`         | `events.valid`     | Выходной топик обогащённых событий                    |
| `KAFKA_TOPIC_DLQ`           | `events.dlq`       | Топик для невалидных событий                          |
| `CLICKHOUSE_ADDR`           | `localhost:9000`   | Адрес ClickHouse (native protocol)                    |
| `CLICKHOUSE_DB`             | `eventflow`        | База данных                                           |
| `CLICKHOUSE_USER`           | `default`          | Пользователь                                          |
| `CLICKHOUSE_PASSWORD`       | _(пусто)_          | Пароль                                                |
| `CLICKHOUSE_BATCH_SIZE`     | `10000`            | Максимальный размер батча перед flush                 |
| `CLICKHOUSE_FLUSH_INTERVAL` | `1s`               | Максимальное время ожидания перед flush               |
| `HTTP_ADDR`                 | `:8080`            | Адрес HTTP-сервера (/healthz, /readyz, /metrics)      |
| `VALIDATOR_WORKERS`         | `4`                | Количество параллельных воркеров (validate + enrich)  |
| `SESSION_IDLE_TIMEOUT`      | `30m`              | Тайм-аут idle-сессии для session stitching            |

---

## HTTP-эндпоинты

| Метод | Путь       | Описание                                                            |
|-------|------------|---------------------------------------------------------------------|
| GET   | `/healthz` | Liveness — всегда `200 OK` если процесс жив                        |
| GET   | `/readyz`  | Readiness — `200` если ClickHouse доступен, иначе `503`            |
| GET   | `/metrics` | Prometheus-метрики в text/plain формате                            |

---

## Prometheus-метрики

| Метрика                                              | Тип       | Описание                                         |
|------------------------------------------------------|-----------|--------------------------------------------------|
| `event_processor_events_consumed_total`              | Counter   | Всего потреблено событий из Kafka (label: project_id) |
| `event_processor_events_valid_total`                 | Counter   | Прошли валидацию и записаны в ClickHouse         |
| `event_processor_events_dlq_total`                   | Counter   | Отправлены в DLQ (label: reason)                 |
| `event_processor_clickhouse_insert_duration_seconds` | Histogram | Длительность batch-вставки в ClickHouse          |
| `event_processor_clickhouse_insert_batch_size`       | Histogram | Размер батча при каждой вставке                  |
| `event_processor_clickhouse_insert_errors_total`     | Counter   | Ошибки вставки в ClickHouse                      |
| `event_processor_kafka_produce_total`                | Counter   | Сообщений опубликовано (label: topic)            |
| `event_processor_kafka_produce_errors_total`         | Counter   | Ошибки публикации (label: topic)                 |
| `event_processor_batch_process_duration_seconds`     | Histogram | Полный цикл батча (от fetch до commit)           |
| `event_processor_session_cache_size`                 | Gauge     | Текущее число активных сессий в памяти           |

---

## Структура проекта

```
event-processor/
├── cmd/
│   └── main.go                   # точка входа, сборка и запуск компонентов
├── internal/
│   ├── config/
│   │   └── config.go             # конфигурация через env-переменные
│   ├── consumer/
│   │   └── consumer.go           # Kafka consumer (segmentio/kafka-go)
│   ├── model/
│   │   └── event.go              # RawEvent, EnrichedEvent, DLQEvent
│   ├── pipeline/
│   │   ├── pipeline.go           # основной ETL-цикл
│   │   ├── validator.go          # валидация полей RawEvent
│   │   └── enricher.go           # UA-парсинг, GeoIP, session stitching
│   ├── producer/
│   │   └── producer.go           # Kafka producer (events.valid / events.dlq)
│   ├── storage/
│   │   └── clickhouse.go         # ClickHouse batch writer
│   ├── metrics/
│   │   └── metrics.go            # Prometheus-метрики
│   ├── server/
│   │   └── server.go             # HTTP-сервер: /healthz /readyz /metrics
│   └── logger/
│       └── logger.go             # zap JSON-логгер
├── deploy/
│   ├── clickhouse/
│   │   └── init.sql              # DDL: events_local + materialized view
│   └── prometheus/
│       └── prometheus.yml        # конфиг scrape для Prometheus
├── tests/
│   ├── test_service.py           # интеграционные тесты (Python)
│   ├── seed.py                   # скрипт для отправки тестовых событий
│   └── requirements.txt          # Python-зависимости
├── Dockerfile                    # multi-stage build
├── docker-compose.yml            # локальный dev-стек
├── Makefile                      # make up / down / logs / test / seed
└── go.mod
```

---

## Схема данных в ClickHouse

```sql
-- Основная таблица (создаётся автоматически при старте ClickHouse)
CREATE TABLE eventflow.events_local
(
    event_id     String,
    project_id   String,
    event_name   LowCardinality(String),
    timestamp    DateTime64(3, 'UTC'),
    user_id      String,
    anonymous_id String,
    session_id   String,
    properties   String CODEC(ZSTD(3)),   -- JSON
    ua_browser   LowCardinality(String),
    ua_os        LowCardinality(String),
    geo_country  LowCardinality(String),
    geo_city     String,
    ingested_at  DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (project_id, event_name, timestamp, user_id)
TTL toDate(timestamp) + INTERVAL 30 DAY;
```

Materialized View `mv_events_per_minute` агрегирует счётчики событий по минутам — используется аналитическим сервисом для быстрых time-series запросов.

---

## Формат RawEvent (Kafka → events.raw)

```json
{
  "event_id":    "01HRT...",
  "project_id":  "proj_abc",
  "event_name":  "page_view",
  "timestamp":   "2026-05-01T12:00:00Z",
  "user_id":     "user_123",
  "anonymous_id": "",
  "properties":  { "url": "/pricing", "referrer": "https://google.com" },
  "context":     { "ua": "Mozilla/5.0 ...", "ip": "203.0.113.1" }
}
```

Обязательные поля: `event_id`, `project_id`, `event_name`, `timestamp`, хотя бы одно из `user_id` / `anonymous_id`.

---

## Устойчивость к сбоям

- **ClickHouse недоступен** — Kafka-оффсеты не коммитятся; события накапливаются в Kafka и будут обработаны после восстановления соединения.
- **Kafka недоступна** — сервис завершает текущие in-flight батчи и возвращает ошибку; при повторном запуске consumer-group продолжает с последнего закоммиченного оффсета.
- **GeoIP-обогащение упало** — поле `geo_country` = `"unknown"`, событие не отбрасывается (graceful degradation).
- **Частично невалидный батч** — валидные события записываются в ClickHouse и `events.valid`; невалидные уходят в `events.dlq`. Оффсет всего батча коммитится в обоих случаях.

---

## Make-команды

```bash
make up            # поднять весь стек (Kafka, ClickHouse, сервис, Prometheus, Grafana)
make down          # остановить и удалить контейнеры
make logs          # tail логов event-processor
make restart       # пересобрать и перезапустить только event-processor
make build         # собрать Go-бинарь локально → bin/event-processor
make docker-build  # собрать Docker-образ
make test          # go test -race ./...
make test-python   # интеграционные тесты (Python)
make seed          # отправить 10 тестовых событий в Kafka
make lint          # golangci-lint run ./...
make tidy          # go mod tidy
make kafka-topics  # пересоздать Kafka-топики вручную
```