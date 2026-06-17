#!/bin/bash
# ============================================================
# GitHub Trending 数据湖 - 健康检查
# 检查所有组件状态
# ============================================================

set -euo pipefail

FAILED=false

check_port() {
    local name=$1 host=$2 port=$3
    if timeout 5 bash -c "echo > /dev/tcp/$host/$port" 2>/dev/null; then
        echo "[PASS] $name ($host:$port)"
    else
        echo "[FAIL] $name ($host:$port)"
        FAILED=true
    fi
}

echo "=== GitHub Trending Lakehouse Health Check ==="
echo "Time: $(date -Iseconds)"
echo ""

# MySQL
check_port MySQL mysql 3306

# Kafka
check_port Kafka kafka 9092

# Flink JobManager
check_port Flink-SQL flink-jobmanager 8081

# StarRocks FE
check_port StarRocks-FE starrocks-fe 9030

# StarRocks BE
check_port StarRocks-BE starrocks-be 8040

# Redis
check_port Redis redis 6379

# Airflow
check_port Airflow airflow 8080

# Superset
check_port Superset superset 8088

echo ""
if [ "$FAILED" = true ]; then
    echo "[WARN] Some components are not reachable!"
    exit 1
else
    echo "[OK] All components are healthy!"
    exit 0
fi
