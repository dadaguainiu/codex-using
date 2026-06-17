#!/usr/bin/env python3
"""
GitHub 实时数据爬虫
采集源: GitHub Trending API + REST API
输出: Kafka Topic (gh_trending_raw / gh_api_raw)
"""

import json
import os
import time
import hashlib
import logging
from datetime import datetime, timezone
from typing import Optional

import requests
from kafka import KafkaProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("github_crawler")

# === Config ===
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC_TRENDING = os.getenv("TOPIC_TRENDING", "gh_trending_raw")
TOPIC_API = os.getenv("TOPIC_API", "gh_api_raw")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "")
INTERVAL_SEC = int(os.getenv("INTERVAL_SEC", "300"))
BATCH_ID_PREFIX = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")


def get_kafka_producer() -> Optional[KafkaProducer]:
    for attempt in range(10):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False, default=str).encode("utf-8"),
                acks="all",
                retries=3,
            )
            log.info("Connected to Kafka at %s", KAFKA_BOOTSTRAP)
            return producer
        except Exception as e:
            log.warning("Kafka connect attempt %d/10 failed: %s", attempt + 1, e)
            time.sleep(5)
    log.error("Failed to connect to Kafka after 10 attempts")
    return None


def fetch_trending() -> list[dict]:
    """采集 GitHub Trending 仓库数据"""
    headers = {"Accept": "application/vnd.github+json"}
    if GITHUB_TOKEN:
        headers["Authorization"] = f"Bearer {GITHUB_TOKEN}"

    repos = []
    for page in range(1, 4):  # 前三页
        url = (
            "https://api.github.com/search/repositories"
            "?q=created:>2026-01-01&sort=stars&order=desc"
            f"&per_page=50&page={page}"
        )
        try:
            resp = requests.get(url, headers=headers, timeout=15)
            if resp.status_code == 403:
                log.warning("Rate limited, sleeping 60s...")
                time.sleep(60)
                continue
            resp.raise_for_status()
            items = resp.json().get("items", [])
            for item in items:
                repos.append({
                    "repo_id": item["id"],
                    "repo_name": item["full_name"],
                    "description": item.get("description", ""),
                    "url": item["html_url"],
                    "language": item.get("language", "Unknown"),
                    "stars": item["stargazers_count"],
                    "forks": item["forks_count"],
                    "daily_stars": 0,
                    "stars_1d": 0,
                    "stars_7d": 0,
                    "topics": item.get("topics", []),
                    "owner": item.get("owner", {}).get("login", ""),
                    "license": item.get("license", {}).get("spdx_id", "") if item.get("license") else "",
                    "is_fork": item.get("fork", False),
                    "created_at": item.get("created_at", ""),
                    "open_issues": item.get("open_issues_count", 0),
                })
            if len(items) < 50:
                break
        except requests.RequestException as e:
            log.error("Trending fetch failed: %s", e)
            break
    return repos


def fetch_repo_details(repo_ids: list[int]) -> list[dict]:
    """采集仓库详细信息（issues / pulls / contributors）"""
    headers = {"Accept": "application/vnd.github+json"}
    if GITHUB_TOKEN:
        headers["Authorization"] = f"Bearer {GITHUB_TOKEN}"

    details = []
    for rid in repo_ids:
        for endpoint, data_type in [
            (f"https://api.github.com/repositories/{rid}/issues?state=all&per_page=5", "issues"),
            (f"https://api.github.com/repositories/{rid}/pulls?state=all&per_page=5", "pulls"),
        ]:
            try:
                resp = requests.get(endpoint, headers=headers, timeout=10)
                if resp.status_code == 403:
                    time.sleep(60)
                    continue
                if resp.status_code == 200:
                    details.append({
                        "repo_id": rid,
                        "data_type": data_type,
                        "payload": resp.json(),
                    })
            except requests.RequestException as e:
                log.warning("API detail fetch failed for repo %s, type %s: %s", rid, data_type, e)
    return details


def build_crawl_batch() -> str:
    return f"{BATCH_ID_PREFIX}_{int(time.time() * 1000)}"


def main():
    producer = get_kafka_producer()
    if not producer:
        return

    log.info("Starting GitHub crawler (interval=%ss)", INTERVAL_SEC)
    while True:
        batch_id = build_crawl_batch()
        event_time = datetime.now(timezone.utc).isoformat()

        # 1) 采集 Trending 数据
        trending_repos = fetch_trending()
        log.info("Fetched %d trending repos", len(trending_repos))

        for repo in trending_repos:
            record = {
                "record_id": hashlib.md5(f"{repo['repo_id']}:{event_time}".encode()).hexdigest(),
                "crawl_batch": batch_id,
                "event_time": event_time,
                "_insert_time": event_time,
                **repo,
            }
            producer.send(TOPIC_TRENDING, value=record)

        # 2) 采集详情数据
        repo_ids = [r["repo_id"] for r in trending_repos[:10]]  # 限制 TOP10
        details = fetch_repo_details(repo_ids)
        log.info("Fetched %d API detail records", len(details))

        for det in details:
            record = {
                "record_id": hashlib.md5(f"{det['repo_id']}:{det['data_type']}:{event_time}".encode()).hexdigest(),
                "crawl_batch": batch_id,
                "event_time": event_time,
                "_insert_time": event_time,
                **det,
            }
            producer.send(TOPIC_API, value=record)

        producer.flush()
        log.info("Batch %s: sent %d + %d records", batch_id, len(trending_repos), len(details))
        time.sleep(INTERVAL_SEC)


if __name__ == "__main__":
    main()
