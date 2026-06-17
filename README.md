# GitHub Trending 实时数据湖平台

> 简历级项目 | 技术栈: Kafka + Flink + Paimon + StarRocks + Redis + dbt + Airflow + Superset

## 项目定位

基于实时数据湖架构构建的 GitHub 趋势项目监控分析平台。自动从 GitHub Trending API / REST API 爬取仓库数据, 经过 Flink 实时流处理写入 Paimon 数据湖, StarRocks 提供 OLAP 热查询加速, 最终通过 Superset 展示可视化大盘。

## 架构概览

```
采集层: Python 爬虫 (5min 轮询) -> Kafka
流计算: Flink SQL -> Paimon (ODS/DWD/DIM/DWS)
热加速: StarRocks (PK 模型) + Redis (Top 100 缓存)
批量建模: dbt + Airflow (日调度)
可视化: Apache Superset
治理: Atlas + lineage.yml + 质量监控
```

## 冷热数据分离

- **热数据 (Redis)**: TOP 100 仓库排行, TTL 5min
- **热查询 (StarRocks)**: 小时级聚合, PK 模型, 响应 <50ms
- **冷查询 (Paimon)**: 全量历史, StarRocks Paimon Catalog 访问, 响应 <2s

## PK vs UK 策略

| 写入方式 | 模型 | 说明 |
|----------|------|------|
| Flink 实时写入 | PK (Primary Key) | Upsert 性能最优, 直接覆盖 |
| dbt 批量写入 | UK (Unique Key) | 保证去重 + 数据质量约束 |

## 目录结构

```
.
|-- docker-compose.yml          # 一键部署
|-- mysql/init.sql              # 元数据初始化
|-- scripts/
|   |-- github_crawler.py       # 爬虫 (Python)
|   +-- health_check.sh         # 健康检查
|-- flink/
|   |-- sql/                    # Flink SQL 作业
|   +-- docker/flink-conf.yaml  # Flink 配置
|-- starrocks/sql/init_ads.sql  # StarRocks 表初始化
|-- dbt/                        # 数据建模
|-- airflow/dags/               # 调度 DAG
|-- superset/dashboard_config.json
|-- lineage.yml                 # 数据血缘
+-- docs/                       # 设计文档
```

## 快速启动

```bash
# 1. 启动所有服务
docker compose up -d

# 2. 等待 all services healthy 后, 初始化 StarRocks 表
docker exec -i gh-sr-fe mysql -h127.0.0.1 -P9030 -uroot < starrocks/sql/init_ads.sql

# 3. 提交 Flink SQL 作业
docker exec -i gh-flink-jm flink run -d -py /opt/flink/sql/01_setup_paimon_catalog.sql

# 4. 配置 Superset
#    打开 http://localhost:8088 (admin/admin)
#    添加 StarRocks 数据源: jdbc:mysql://starrocks-fe:9030

# 5. 访问可视化
#    Superset: http://localhost:8088
#    Airflow:  http://localhost:8082 (admin/admin)
#    Flink UI: http://localhost:8081
```

## 访问地址

| 组件 | 地址 | 认证 |
|------|------|------|
| Superset | http://localhost:8088 | admin/admin |
| Airflow | http://localhost:8082 | admin/admin |
| Flink Web UI | http://localhost:8081 | - |
| StarRocks SQL | mysql -h127.0.0.1 -P9030 -uroot | - |
