# app/notifier.py
import os
import redis
import logging
import json
from datetime import datetime, timedelta
from typing import List, Dict, Any
from elasticsearch import Elasticsearch
from kakao import KakaoMessage
from utils import get_redis_client, get_es_client

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def get_search_keywords(redis_client: redis.Redis,
                        config: Dict[str, Any], 
                        ) -> List[str]:
    """
    Retrieves search keywords from Redis or a fallback file.

    - First, attempts to retrieve stored keywords from Redis.
    - If no keywords are found, loads them from `config.json`.

    :param redis_client: Redis client instance for fetching keywords.
    :return: List of keywords for search.
    """
    keywords = list(redis_client.smembers("search_keywords"))
    
    if not keywords and config["keywords"]:
        keywords = config["keywords"]

    if keywords:
        logger.info(f"Loaded {len(keywords)} keywords: {keywords}")
    else:
        logger.warning("⚠️ No search keywords found.")
    
    return keywords

def search_papers(es_client: Elasticsearch, 
                  keywords: List[str], 
                  last_crawl_time: str
                  ) -> Dict[str, List[Dict[str, Any]]]:
    """
    Queries Elasticsearch for research papers matching the given keywords.

    - Uses the latest crawl timestamp from Redis to ensure up-to-date results.
    - Filters papers by their abstract content and publication date.

    :param es_client: Elasticsearch client instance.
    :param keywords: List of search keywords.
    :param last_crawl_time: UTC timestamp of the last successful crawl.
    :return: Dictionary mapping keywords to lists of relevant research papers.
    """
    if not keywords:
        logger.warning("⚠️ No search keywords provided.")
        return {}

    notifications = {}
    last_crawl_dt = datetime.utcnow() - timedelta(hours=24)

    for keyword in keywords:
        query_body = {
            "query": {
                "bool": {
                    "must": [
                        {"match": {"abstract": keyword}},  
                        {"range": {"published_date": {"gte": last_crawl_dt.isoformat()}}}
                    ]
                }
            }
        }

        response = es_client.search(index="arxiv_papers", body=query_body)
        notifications[keyword] = [hit["_source"] for hit in response["hits"]["hits"]]

    return notifications

def send_notification(config: Dict[str, Any]) -> None:
    """
    Searches for relevant research papers and sends notifications via KakaoTalk.

    - Load keywords from Redis.
    - Queries Elasticsearch for matching papers.
    - Sends notifications through KakaoTalk.

    :param config: Configuration dictionary containing system settings.
    """
    redis_client = get_redis_client()
    es_client = get_es_client()

    # Initialize messenger
    messenger_type = config.get("messenger", "kakao").lower()  # Default: KakaoTalk
    if messenger_type == "kakao":
        messenger = KakaoMessage(config)
    else:
        logger.warning(f"⚠️ Messenger '{messenger_type}' is not supported yet.")
        return

    # Load keywords and last crawl timestamp
    keywords = get_search_keywords(redis_client, config)
    last_crawl_time = redis_client.get("last_crawl_timestamp")  # Retrieve last crawl timestamp

    # Search for relevant papers (e.g. 1st Retrieval : BM25 in Elasticsearch)
    notifications = search_papers(es_client, keywords, last_crawl_time)

    # Send notifications via the selected messenger
    for keyword, papers in notifications.items():
        if not papers:
            logger.info(f"🔍 No new papers found for keyword: '{keyword}'")
            continue

        logger.info(f"🔔 Sending {len(papers)} notifications for '{keyword}' since {last_crawl_time}.")

        if messenger_type == "kakao":
            response = messenger.send_paper_kakao(config, keyword, papers)  # Unified send method
        else:
            logger.warning(f"⚠️ Messenger '{messenger_type}' is not supported yet.")
            return

        if response.status_code == 200:
            logger.info(f"✅ Successfully sent {messenger_type} notifications for keyword: '{keyword}'")
        else:
            logger.warning(f"❌ Failed to send {messenger_type} notifications for '{keyword}': {response.json()}")

if __name__ == "__main__":
    from tasks import load_config  # Import configuration loader
    config = load_config()
    send_notification(config)