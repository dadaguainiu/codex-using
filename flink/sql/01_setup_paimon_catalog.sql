-- ============================================================
-- Flink SQL: Paimon Catalog + ODS 表创建
-- 在 Flink SQL Client 中执行: source /opt/flink/sql/01_setup_paimon_catalog.sql
-- ============================================================

-- 创建 Paimon Catalog
CREATE CATALOG paimon_catalog WITH (
    'type' = 'paimon',
    'warehouse' = 'file:/data/paimon/warehouse'
);

USE CATALOG paimon_catalog;

-- 创建 ODS 层数据库
CREATE DATABASE IF NOT EXISTS ods;
USE ods;

-- GitHub Trending 原始数据 (append-only)
CREATE TABLE IF NOT EXISTS ods_trending_raw (
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
    raw_json        STRING,
    _insert_time    TIMESTAMP(3) METADATA,
    WATERMARK FOR _insert_time AS _insert_time - INTERVAL '10' SECOND,
    PRIMARY KEY (record_id) NOT ENFORCED
) WITH (
    'bucket' = '4',
    'write-mode' = 'append-only',
    'changelog-producer' = 'none',
    'snapshot.time-retained' = '7d'
);

-- GitHub API 原始数据 (append-only)
CREATE TABLE IF NOT EXISTS ods_github_api_raw (
    record_id       STRING,
    repo_id         BIGINT,
    data_type       STRING,
    payload         STRING,
    crawl_batch     STRING,
    event_time      STRING,
    _insert_time    TIMESTAMP(3) METADATA,
    WATERMARK FOR _insert_time AS _insert_time - INTERVAL '10' SECOND,
    PRIMARY KEY (record_id) NOT ENFORCED
) WITH (
    'bucket' = '2',
    'write-mode' = 'append-only',
    'changelog-producer' = 'none',
    'snapshot.time-retained' = '7d'
);

-- 创建 DWD 层数据库
CREATE DATABASE IF NOT EXISTS dwd;
USE dwd;

-- 仓库每日快照 (upsert)
CREATE TABLE IF NOT EXISTS dwd_repo_snapshot_di (
    repo_id             BIGINT,
    repo_name           STRING,
    description         STRING,
    url                 STRING,
    primary_language    STRING,
    stars               BIGINT,
    forks               BIGINT,
    daily_stars         BIGINT,
    stars_1d            BIGINT,
    stars_7d            BIGINT,
    topics              ARRAY<STRING>,
    open_issues         BIGINT,
    closed_issues       BIGINT,
    contributor_count   BIGINT,
    snapshot_date       STRING,
    etl_time            TIMESTAMP(3),
    PRIMARY KEY (repo_id, snapshot_date) NOT ENFORCED
) WITH (
    'bucket' = '4',
    'changelog-producer' = 'input',
    'snapshot.time-retained' = '30d'
);

-- Stars 变更流水 (upsert)
CREATE TABLE IF NOT EXISTS dwd_repo_stars_changelog_di (
    repo_id         BIGINT,
    repo_name       STRING,
    language        STRING,
    stars_before    BIGINT,
    stars_after     BIGINT,
    stars_delta     BIGINT,
    snapshot_date   STRING,
    event_time      TIMESTAMP(3),
    PRIMARY KEY (repo_id, event_time) NOT ENFORCED
) WITH (
    'bucket' = '4',
    'changelog-producer' = 'input',
    'snapshot.time-retained' = '30d'
);

-- 创建 DIM 层数据库
CREATE DATABASE IF NOT EXISTS dim;
USE dim;

-- 编程语言维度 (SCD Type 1)
CREATE TABLE IF NOT EXISTS dim_language (
    language_id     BIGINT,
    language_name   STRING,
    category        STRING,
    is_mainstream   BOOLEAN,
    first_seen_date STRING,
    etl_time        TIMESTAMP(3),
    PRIMARY KEY (language_id) NOT ENFORCED
) WITH (
    'bucket' = '1',
    'changelog-producer' = 'input'
);

-- 仓库维度 (SCD Type 1)
CREATE TABLE IF NOT EXISTS dim_repo (
    repo_id             BIGINT,
    repo_name           STRING,
    owner               STRING,
    full_name           STRING,
    description         STRING,
    url                 STRING,
    topics              ARRAY<STRING>,
    license             STRING,
    is_archived         BOOLEAN,
    is_fork             BOOLEAN,
    created_at          STRING,
    first_trending_date STRING,
    etl_time            TIMESTAMP(3),
    PRIMARY KEY (repo_id) NOT ENFORCED
) WITH (
    'bucket' = '4',
    'changelog-producer' = 'input'
);
