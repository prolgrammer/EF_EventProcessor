-- ClickHouse schema initialisation for event-processor (single-node dev).
-- Run automatically when the container starts via /docker-entrypoint-initdb.d/

CREATE DATABASE IF NOT EXISTS eventflow;

CREATE TABLE IF NOT EXISTS eventflow.events_local
(
    event_id        String,
    project_id      String,
    event_name      LowCardinality(String),
    timestamp       DateTime64(3, 'UTC'),
    user_id         String,
    anonymous_id    String,
    session_id      String,
    properties      String CODEC(ZSTD(3)),
    ua_browser      LowCardinality(String),
    ua_os           LowCardinality(String),
    geo_country     LowCardinality(String),
    geo_city        String,
    ingested_at     DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (project_id, event_name, timestamp, user_id)
TTL toDate(timestamp) + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;

-- Materialised view: per-minute event counts used by the analytics service.
CREATE TABLE IF NOT EXISTS eventflow.mv_events_per_minute_data
(
    project_id LowCardinality(String),
    event_name LowCardinality(String),
    ts         DateTime,
    cnt        UInt64
)
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY (project_id, event_name, ts);

CREATE MATERIALIZED VIEW IF NOT EXISTS eventflow.mv_events_per_minute
TO eventflow.mv_events_per_minute_data
AS
SELECT
    project_id,
    event_name,
    toStartOfMinute(timestamp) AS ts,
    count()                    AS cnt
FROM eventflow.events_local
GROUP BY project_id, event_name, ts;
