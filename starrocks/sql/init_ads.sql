-- ============================================================
-- StarRocks ADS 层初始化
-- 热表: DWS (PK), ADS (UK)
-- ============================================================

-- DWS 层: 仓库小时聚合 (PK 模型, Flink 实时写入)
CREATE DATABASE IF NOT EXISTS dws;

CREATE TABLE IF NOT EXISTS dws.dws_repo_hourly_agg (
    repo_id             BIGINT NOT NULL,
    repo_name           VARCHAR(256) NOT NULL,
    language            VARCHAR(64),
    hour_slot           VARCHAR(16) NOT NULL COMMENT 'yyyy-MM-dd/HH',
    current_stars       BIGINT DEFAULT 0,
    stars_increment     BIGINT DEFAULT 0,
    forks_increment     BIGINT DEFAULT 0,
    rank_by_stars       INT DEFAULT 999999,
    rank_by_language    INT DEFAULT 999999,
    update_time         DATETIME DEFAULT CURRENT_TIMESTAMP
) ENGINE=OLAP
PRIMARY KEY (repo_id, hour_slot)
DISTRIBUTED BY HASH(repo_id) BUCKETS 8
PARTITION BY RANGE(str2date(left(hour_slot,10), "%Y-%m-%d")) (
    PARTITION p202601 VALUES [('2026-01-01'), ('2026-02-01')),
    PARTITION p202602 VALUES [('2026-02-01'), ('2026-03-01')),
    PARTITION p202603 VALUES [('2026-03-01'), ('2026-04-01')),
    PARTITION p202604 VALUES [('2026-04-01'), ('2026-05-01')),
    PARTITION p202605 VALUES [('2026-05-01'), ('2026-06-01')),
    PARTITION p202606 VALUES [('2026-06-01'), ('2026-07-01')),
    PARTITION p202607 VALUES [('2026-07-01'), ('2026-08-01')),
    PARTITION p202608 VALUES [('2026-08-01'), ('2026-09-01')),
    PARTITION p202609 VALUES [('2026-09-01'), ('2026-10-01')),
    PARTITION p202610 VALUES [('2026-10-01'), ('2026-11-01')),
    PARTITION p202611 VALUES [('2026-11-01'), ('2026-12-01')),
    PARTITION p202612 VALUES [('2026-12-01'), ('2027-01-01'))
)
PROPERTIES (
    "replication_num" = "1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "MONTH",
    "dynamic_partition.start" = "-3",
    "dynamic_partition.end" = "1",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "8"
);

-- ADS 层: 分析应用表
CREATE DATABASE IF NOT EXISTS ads;

-- 语言热度趋势 (UK 模型, dbt 批量写入)
CREATE TABLE IF NOT EXISTS ads.ads_language_trend_1d (
    stat_date           DATE NOT NULL,
    language            VARCHAR(64) NOT NULL,
    repo_cnt            INT DEFAULT 0,
    total_stars         BIGINT DEFAULT 0,
    avg_stars           DOUBLE DEFAULT 0.0,
    top_repo            VARCHAR(256) DEFAULT '',
    stars_growth_rate   DOUBLE DEFAULT 0.0,
    etl_time            DATETIME DEFAULT CURRENT_TIMESTAMP
) ENGINE=OLAP
DUPLICATE KEY(stat_date, language)
DISTRIBUTED BY HASH(stat_date) BUCKETS 4
PROPERTIES ("replication_num" = "1");

-- 全局指标 (PK 模型)
CREATE TABLE IF NOT EXISTS ads.ads_global_metrics_1d (
    stat_date               DATE NOT NULL,
    total_trending_repos    INT DEFAULT 0,
    total_stars_added       BIGINT DEFAULT 0,
    total_forks_added       BIGINT DEFAULT 0,
    avg_daily_stars         DOUBLE DEFAULT 0.0,
    hottest_language        VARCHAR(64) DEFAULT '',
    fastest_growing_repo    VARCHAR(256) DEFAULT '',
    etl_time                DATETIME DEFAULT CURRENT_TIMESTAMP
) ENGINE=OLAP
PRIMARY KEY (stat_date)
DISTRIBUTED BY HASH(stat_date) BUCKETS 1
PROPERTIES ("replication_num" = "1");

-- ============================================================
-- Paimon Catalog 注册 (冷查询)
-- ============================================================
-- 在 StarRocks 中执行以下 SQL 注册 Paimon Catalog
-- CREATE EXTERNAL CATALOG paimon_catalog
-- PROPERTIES (
--     "type" = "paimon",
--     "paimon.catalog.type" = "filesystem",
--     "paimon.catalog.warehouse" = "file:/data/paimon/warehouse"
-- );
