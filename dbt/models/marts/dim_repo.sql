-- dbt marts: 仓库维度表
WITH repo_source AS (
    SELECT
        repo_id,
        repo_name,
        owner,
        CONCAT(owner, '/', repo_name) AS full_name,
        description,
        url,
        topics,
        license,
        is_fork,
        MIN(snapshot_date) AS first_trending_date,
        MAX(snapshot_date) AS last_seen_date
    FROM {{ ref('stg_repo_daily') }}
    GROUP BY repo_id, repo_name, owner, description, url, topics, license, is_fork
)

SELECT
    repo_id,
    repo_name,
    owner,
    full_name,
    description,
    url,
    topics,
    license,
    is_fork,
    first_trending_date,
    CURRENT_TIMESTAMP AS etl_time
FROM repo_source
