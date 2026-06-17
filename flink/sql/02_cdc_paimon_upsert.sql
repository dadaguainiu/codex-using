-- ============================================================
-- Flink SQL: CDC 实时写入 - Kafka -> Paimon ODS -> DWD
-- ============================================================

USE CATALOG paimon_catalog;

-- 1) 创建 Kafka Source Table (gh_trending_raw)
DROP TEMPORARY TABLE IF EXISTS kafka_trending_source;
CREATE TEMPORARY TABLE kafka_trending_source (
    record_id       STRING,
    repo_id         BIGINT,
    repo_name       STRING,
    description     STRING,
    url             STRING,
    language        STRING,
    stars           BIGINT,
    forks           BIGINT,
    daily_stars     BIGINT,
    stars_1d        BIGINT,
    stars_7d        BIGINT,
    topics          ARRAY<STRING>,
    owner           STRING,
    license         STRING,
    is_fork         BOOLEAN,
    created_at      STRING,
    open_issues     BIGINT,
    crawl_batch     STRING,
    event_time      STRING,
    _insert_time    TIMESTAMP(3) METADATA FROM 'timestamp',
    proc_time       AS PROCTIME(),
    WATERMARK FOR _insert_time AS _insert_time - INTERVAL '10' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'gh_trending_raw',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink-cdc-group',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);

-- 2) 流式写入 ODS (append-only)
INSERT INTO ods.ods_trending_raw
SELECT
    record_id, repo_id, repo_name, description, url,
    COALESCE(language, 'Unknown'), stars, forks, daily_stars,
    stars_1d, stars_7d, topics, owner,
    COALESCE(license, ''), is_fork, created_at, open_issues,
    crawl_batch, event_time,
    CAST(NULL AS STRING) AS raw_json,
    _insert_time
FROM kafka_trending_source;

-- 3) DWD: 从 ODS 聚合写入仓库快照
INSERT INTO dwd.dwd_repo_snapshot_di
SELECT
    repo_id,
    repo_name,
    description,
    url,
    language AS primary_language,
    stars,
    forks,
    daily_stars,
    stars_1d,
    stars_7d,
    topics,
    open_issues,
    CAST(0 AS BIGINT) AS closed_issues,
    CAST(0 AS BIGINT) AS contributor_count,
    DATE_FORMAT(_insert_time, 'yyyy-MM-dd') AS snapshot_date,
    _insert_time AS etl_time
FROM ods.ods_trending_raw
WHERE _insert_time IS NOT NULL;
