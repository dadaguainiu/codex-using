-- dbt staging: 从 ODS 提取仓库日数据
WITH source AS (
    SELECT
        repo_id,
        repo_name,
        language,
        stars,
        forks,
        topics,
        description,
        url,
        owner,
        license,
        is_fork,
        CAST(crawl_batch AS CHAR) AS source_batch,
        CAST(SUBSTRING(event_time, 1, 10) AS DATE) AS snapshot_date
    FROM ods.ods_trending_raw
    WHERE event_time IS NOT NULL
)

SELECT DISTINCT
    repo_id,
    MAX(repo_name) AS repo_name,
    MAX(language) AS language,
    MAX(stars) AS stars,
    MAX(forks) AS forks,
    MAX(topics) AS topics,
    MAX(description) AS description,
    MAX(url) AS url,
    MAX(owner) AS owner,
    MAX(license) AS license,
    MAX(is_fork) AS is_fork,
    MAX(source_batch) AS source_batch,
    snapshot_date
FROM source
GROUP BY repo_id, snapshot_date
