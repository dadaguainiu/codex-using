-- ============================================================
-- Flink SQL: DWS 聚合 + StarRocks 写入
-- ============================================================

USE CATALOG paimon_catalog;

-- 创建 DWS 层数据库
CREATE DATABASE IF NOT EXISTS dws;
USE dws;

-- Paimon DWS: 仓库日聚合 (冷存储)
CREATE TABLE IF NOT EXISTS dws_repo_daily_agg (
    repo_id             BIGINT,
    repo_name           STRING,
    language            STRING,
    total_stars         BIGINT,
    daily_stars_delta   BIGINT,
    total_forks         BIGINT,
    daily_forks_delta   BIGINT,
    open_issues_cnt     BIGINT,
    avg_response_hours  DOUBLE,
    contributor_cnt     BIGINT,
    stat_date           STRING,
    etl_time            TIMESTAMP(3),
    PRIMARY KEY (repo_id, stat_date) NOT ENFORCED
) WITH (
    'bucket' = '4',
    'changelog-producer' = 'full-compaction',
    'full-compaction.delta-commits' = '5',
    'snapshot.time-retained' = '90d'
);

-- 日聚合插入 (从 DWD 按天聚合)
INSERT INTO dws.dws_repo_daily_agg
SELECT
    repo_id,
    MAX(repo_name) AS repo_name,
    MAX(primary_language) AS language,
    MAX(stars) AS total_stars,
    MAX(stars) - MIN(stars) AS daily_stars_delta,
    MAX(forks) AS total_forks,
    MAX(forks) - MIN(forks) AS daily_forks_delta,
    MAX(open_issues) AS open_issues_cnt,
    CAST(0.0 AS DOUBLE) AS avg_response_hours,
    MAX(contributor_count) AS contributor_cnt,
    snapshot_date AS stat_date,
    MAX(etl_time) AS etl_time
FROM dwd.dwd_repo_snapshot_di
GROUP BY repo_id, snapshot_date;

-- ============================================================
-- StarRocks 热表写入 (通过 JDBC connector)
-- ============================================================

-- 注册 StarRocks 表 (JDBC)
CREATE TEMPORARY TABLE sr_dws_repo_hourly_agg (
    repo_id             BIGINT,
    repo_name           STRING,
    language            STRING,
    hour_slot           STRING,
    current_stars       BIGINT,
    stars_increment     BIGINT,
    forks_increment     BIGINT,
    rank_by_stars       BIGINT,
    rank_by_language    BIGINT,
    update_time         TIMESTAMP(3),
    PRIMARY KEY (repo_id, hour_slot) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://starrocks-fe:9030/dws',
    'table-name' = 'dws_repo_hourly_agg',
    'username' = 'root',
    'password' = '',
    'sink.buffer-flush.max-rows' = '1000',
    'sink.buffer-flush.interval' = '10s'
);

-- 从 ODS 流式写入 StarRocks DWS (模拟 CDC)
INSERT INTO sr_dws_repo_hourly_agg
SELECT
    repo_id,
    repo_name,
    language,
    DATE_FORMAT(_insert_time, 'yyyy-MM-dd/HH') AS hour_slot,
    stars AS current_stars,
    stars_1d AS stars_increment,
    forks - 0 AS forks_increment,
    CAST(0 AS BIGINT) AS rank_by_stars,
    CAST(0 AS BIGINT) AS rank_by_language,
    _insert_time AS update_time
FROM ods.ods_trending_raw;
