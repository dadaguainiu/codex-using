-- ============================================================
-- MySQL 元数据初始化
-- 用于存储 Flink CDC / dbt 所需的元数据信息
-- ============================================================

-- 爬虫配置表
CREATE TABLE IF NOT EXISTS crawler_config (
    id          INT AUTO_INCREMENT PRIMARY KEY,
    source      VARCHAR(64)   NOT NULL COMMENT '数据源: github_trending / github_api',
    api_url     VARCHAR(512)  NOT NULL,
    interval_sec INT NOT NULL DEFAULT 300 COMMENT '轮询间隔(秒)',
    is_active   TINYINT       NOT NULL DEFAULT 1,
    created_at  DATETIME      DEFAULT CURRENT_TIMESTAMP,
    updated_at  DATETIME      DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_source (source)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='爬虫配置';

-- 数据质量检查结果表
CREATE TABLE IF NOT EXISTS quality_check_log (
    id          BIGINT AUTO_INCREMENT PRIMARY KEY,
    check_name  VARCHAR(128)  NOT NULL COMMENT '检查名称',
    status      ENUM('PASS','WARN','FAIL') NOT NULL DEFAULT 'PASS',
    detail      JSON,
    checked_at  DATETIME      DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_checked_at (checked_at),
    INDEX idx_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='数据质量检查日志';

-- Flink 作业管理表
CREATE TABLE IF NOT EXISTS flink_job_log (
    id          BIGINT AUTO_INCREMENT PRIMARY KEY,
    job_id      VARCHAR(64)   NOT NULL,
    job_name    VARCHAR(128)  NOT NULL,
    status      VARCHAR(32)   NOT NULL DEFAULT 'INIT',
    started_at  DATETIME,
    ended_at    DATETIME,
    created_at  DATETIME      DEFAULT CURRENT_TIMESTAMP,
    updated_at  DATETIME      DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_job_id (job_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Flink 作业日志';

-- 数据血缘记录表
CREATE TABLE IF NOT EXISTS lineage_log (
    id          BIGINT AUTO_INCREMENT PRIMARY KEY,
    source_table VARCHAR(128) NOT NULL,
    target_table VARCHAR(128) NOT NULL,
    relation    VARCHAR(32)   NOT NULL COMMENT 'ETL / VIEW / FK',
    batch_id    VARCHAR(64),
    etl_time    DATETIME      DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_source (source_table),
    INDEX idx_target (target_table)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='数据血缘日志';

-- 初始配置数据
INSERT INTO crawler_config (source, api_url, interval_sec) VALUES
    ('github_trending', 'https://api.github.com/search/repositories?q=created:>2026-01-01&sort=stars&order=desc', 300),
    ('github_trending_daily', 'https://github.com/trending', 3600),
    ('github_api_trending', 'https://api.github.com/repositories', 600)
ON DUPLICATE KEY UPDATE api_url=VALUES(api_url);
