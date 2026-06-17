# GitHub 实时数据湖平台

> 设计规范 v1.0 | 2026-06-18

## 项目定位

基于 Kafka + Flink + Paimon + StarRocks 构建 GitHub Trending 实时数据湖平台。
数据来源：GitHub Trending API + REST API 爬虫
数据流向：爬虫采集 -> 消息队列 -> OLAP 分析 -> 可视化大屏
覆盖实时流处理 + 批量调度 + 数据治理全链路。

### 核心目标
- 实时采集 GitHub Trending / REST API 数据 -> Kafka -> Flink -> Paimon/StarRocks
- 支持秒级实时聚合与 Upsert 语义
- 通过 Superset 提供可视化大盘
- 完整的数据血缘、质量监控与治理

## 技术选型

| 层级 | 组件 | 版本 | 部署方式 |
|------|------|------|----------|
| 数据采集 | Python 爬虫 | GitHub Trending API + REST API 定时采集 | Docker |
| 消息队列 | Apache Kafka (KRaft) | 3.7+ | Docker |
| 流计算 | Apache Flink + Flink SQL | 1.19+ | Docker |
| 数据湖 | Apache Paimon | 0.8+ | 本地卷 |
| OLAP 查询 | StarRocks | 3.3+ | Docker |
| 热缓存 | Redis | 7.x | Docker |
| 搜索缓存 | Elasticsearch | (架构示意, 暂不部署) | (注释) |
| 数据建模 | dbt-core | 1.8+ | Docker |
| 任务调度 | Apache Airflow | 2.10+ | Docker |
| 可视化 | Apache Superset | 4.0+ | Docker |
| 数据治理 | Atlas 架构示意 + lineage.yml + 质量监控 | 1.x | (内建) |

## 架构设计

### 3.1 数据流

GitHub Trending API / REST API -> Python Crawler (5min 轮询) -> Kafka -> Flink SQL

Flink 解析 JSON 消息 -> 维表关联 -> 实时聚合 -> 双写 -> 存储

写路径策略：
- Paimon: ODS (append-only) + DWD (upsert) + DWS (聚合快照)
- StarRocks: DWS PK 模型热加速 + ADS UK 模型, Paimon Catalog 查冷数据
- Redis: TOP100 热点缓存 + 增量排行 TTL 5min

批路径：dbt -> Airflow 00:30 dbt_run / 01:00 quality_check / 01:30 compaction

### 3.2 查询路径

请求 -> Redis 缓存命中 (响应 <1ms)
  |-> StarRocks 热查询 (响应 <50ms)
  |-> StarRocks Paimon Catalog 冷查询 (响应 <2s)

### 3.3 PK vs UK 规则

- Flink 实时写入 -> PK 模型 (Paimon / StarRocks)
- dbt / Airflow 批量写入 -> UK 模型 (StarRocks)
- PK 保证 upsert 性能, UK 保证批量去重和数据质量约束

## 表结构设计

### 4.1 ODS 层 (Paimon append-only)

**ods_trending_raw** — GitHub Trending 原始数据
字段: record_id(UUID), repo_id, repo_name, description, url, language, stars, forks, daily_stars, stars_1d, stars_7d, topics(array), event_time, crawl_batch, raw_json, _insert_time
分区: append-only, bucket=4

**ods_github_api_raw** — GitHub REST API 原始数据
字段: record_id, repo_id, data_type(issues|pulls|contributors), payload(JSON), event_time, crawl_batch, _insert_time
分区: append-only, bucket=2

### 4.2 DWD 层 (Paimon changelog Upsert)

**dwd_repo_snapshot_di** — 仓库每日快照
PK: (repo_id, snapshot_date)
字段: repo_id, repo_name, description, url, primary_language, stars, forks, daily_stars, stars_1d, stars_7d, topics(array), open_issues, closed_issues, contributor_count, snapshot_date, etl_time
分区: changelog-producer=input, bucket=4

**dwd_repo_stars_changelog_di** — Stars 变更流水
PK: (repo_id, event_time)
字段: repo_id, repo_name, language, stars_before, stars_after, stars_delta, snapshot_date, event_time
分区: changelog-producer=input, bucket=4

### 4.3 DIM 层 (Paimon SCD Type 1)

**dim_language** — 编程语言维度
PK: (language_id)
字段: language_id, language_name, category, is_mainstream, first_seen_date, etl_time

**dim_repo** — 仓库维度
PK: (repo_id)
字段: repo_id, repo_name, owner, full_name, description, url, topics, license, is_archived, is_fork, created_at, first_trending_date, etl_time

### 4.4 DWS 层 (StarRocks PK + Paimon 聚合)

**StarRocks: dws_repo_hourly_agg** — 仓库小时聚合 (热)
PK: (repo_id, hour_slot)
字段: repo_id, repo_name, language, hour_slot, current_stars, stars_increment, forks_increment, rank_by_stars, rank_by_language, update_time
分区: RANGE(hour_slot) 按月分区

**Paimon: dws_repo_daily_agg** — 仓库日聚合 (冷)
PK: (repo_id, stat_date)
字段: repo_id, repo_name, language, total_stars, daily_stars_delta, total_forks, daily_forks_delta, open_issues_cnt, avg_response_hours, contributor_cnt
分区: changelog-producer=full-compaction

### 4.5 ADS 层 (StarRocks UK 模型)

**ads_language_trend_1d** — 语言热度趋势
UK: (stat_date, language)
字段: stat_date, language, repo_cnt, total_stars, avg_stars, top_repo, stars_growth_rate, etl_time

**ads_global_metrics_1d** — 全局指标
PK: (stat_date)
字段: stat_date, total_trending_repos, total_stars_added, total_forks_added, avg_daily_stars, hottest_language, fastest_growing_repo, etl_time

### 4.6 Redis 缓存设计

- top:repos:global (ZSET, repo_id->stars_delta, TTL=5min)
- top:repos:{language} (ZSET, repo_id->stars_delta, TTL=5min)
- repo:detail:{repo_id} (HASH, TTL=5min)
- repo:trend:increment (ZSET, repo_id->5min_delta, TTL=5min)

## 数据治理体系

### 5.1 数据血缘

- ODS: crawl_batch + _insert_time 标记来源
- DWD/DWS/ADS: 每行包含 etl_time + source_batch 溯源字段
- 提供 lineage.yml 描述表间依赖关系
- ADS -> etl_time -> DWD -> crawl_batch -> ODS raw_json

### 5.2 数据质量

| 检查点 | 规则 | 报警阈值 |
|--------|------|----------|
| Flink 实时 | SQL 校验 + ROW_NUMBER 去重 | >30s / >5min |
| StarRocks | NOT NULL p99 / 唯一性 TPS | >1s |
| Kafka | 消费者延迟 | >10000 |
| Redis | 缓存命中率 | <80% |
| Airflow | DAG 成功率 | <100% |
| Paimon | 快照过期 | >1000 |

### 5.3 性能监控

scripts/health_check.sh 每小时检查所有组件状态

## Docker Compose 服务

| 服务 | 端口 | 说明 |
|------|------|------|
| MySQL | 3306 | 元数据存储 |
| Kafka | 9092 | KRaft 模式无 ZK |
| Flink JobManager | 8081 | Flink Web UI |
| StarRocks FE | 9030/8030 | SQL 接口 / HTTP |
| StarRocks BE | 8040 | 计算节点 |
| Redis | 6379 | 热缓存 |
| Airflow | 8082 | Web UI |
| Superset | 8088 | 可视化 |
| ES | 9200 | (架构注释, 暂不部署) |

## 项目目录结构

```
github-trending-lakehouse/
|-- docker-compose.yml
|-- .gitignore
|-- README.md
|-- mysql/
|   +-- init.sql
|-- scripts/
|   +-- github_crawler.py
|   +-- health_check.sh
|-- flink/
|   |-- sql/
|   |   |-- 01_setup_paimon_catalog.sql
|   |   |-- 02_cdc_paimon_upsert.sql
|   |   +-- 03_dws_agg.sql
|   +-- docker/
|       +-- Dockerfile
|-- starrocks/
|   +-- sql/init_ads.sql
|-- paimon/ (本地卷挂载)
|-- dbt/
|   |-- dbt_project.yml
|   |-- profiles.yml
|   +-- models/
|       |-- staging/schema.yml + stg_repo_daily.sql
|       +-- marts/dim_repo.sql + fact_repo_hourly.sql
|-- airflow/dags/
|   |-- dbt_run_dag.py
|   +-- data_quality_dag.py
|-- superset/dashboard_config.json
|-- docs/
|   +-- superpowers/specs/2026-06-18-realtime-lakehouse-design.md
+-- lineage.yml
```
