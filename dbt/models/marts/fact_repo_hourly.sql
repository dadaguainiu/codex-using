-- dbt marts: 仓库小时聚合事实表
WITH hourly_data AS (
    SELECT
        repo_id,
        repo_name,
        language,
        stars,
        forks,
        snapshot_date,
        ROW_NUMBER() OVER (
            PARTITION BY repo_id, snapshot_date
            ORDER BY stars DESC
        ) AS rn
    FROM {{ ref('stg_repo_daily') }}
)

SELECT
    repo_id,
    repo_name,
    language,
    CONCAT(CAST(snapshot_date AS CHAR), '/00') AS hour_slot,
    MAX(stars) AS current_stars,
    MAX(stars) - MIN(stars) AS stars_increment,
    MAX(forks) - MIN(forks) AS forks_increment,
    ROW_NUMBER() OVER (ORDER BY MAX(stars) DESC) AS rank_by_stars,
    ROW_NUMBER() OVER (
        PARTITION BY language
        ORDER BY MAX(stars) DESC
    ) AS rank_by_language,
    CURRENT_TIMESTAMP AS etl_time
FROM hourly_data
WHERE rn = 1
GROUP BY repo_id, repo_name, language, snapshot_date
