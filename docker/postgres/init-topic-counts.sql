-- Run automatically by the official postgres image on first container startup (see
-- docker-compose.yml: mounted under /docker-entrypoint-initdb.d/). Not re-run on later
-- restarts, since Postgres only executes these scripts against an empty data directory.
--
-- One row per (window_start, window_end, text): SparkAggregationApp's foreachBatch upserts on
-- this same triple, so it doubles as the table's idempotency key.
CREATE TABLE IF NOT EXISTS topic_counts (
    window_start TIMESTAMP NOT NULL,
    window_end   TIMESTAMP NOT NULL,
    text         TEXT NOT NULL,
    count        BIGINT NOT NULL,
    PRIMARY KEY (window_start, window_end, text)
);
