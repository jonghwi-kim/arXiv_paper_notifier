import redis
import logging
import json
from typing import  Dict, Any, List
from datetime import datetime
from elasticsearch import Elasticsearch
from config import ELASTIC_HOST, ELASTIC_ID, ELASTIC_PASSWORD, REDIS_HOST

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def get_redis_client() -> redis.Redis:
    """
    Establishes a connection to the Redis server.

    :param config: Configuration dictionary containing Redis settings.
    :return: Redis client instance.
    """
    return redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)

def get_es_client() -> Elasticsearch:
    """
    Establishes a connection to the Elasticsearch server.

    :param config: Configuration dictionary containing Elasticsearch settings.
    :return: Elasticsearch client instance.
    """
    return Elasticsearch(ELASTIC_HOST, basic_auth=(ELASTIC_ID, ELASTIC_PASSWORD))

def load_config() -> Dict[str, Any]:
    """Load configuration from config.json."""
    with open("config.json", "r", encoding="utf-8") as file:
        return json.load(file)

def store_config_in_redis(redis_client: redis.Redis, 
                          config: Dict[str, Any]
                          ) -> None:
    """
    Store the entire configuration dictionary in Redis as a JSON string.
    
    :param config: Redis client instance
    :param config: Configuration dictionary containing Redis settings.

    Purpose:
      - Makes all configuration variables accessible to tasks.
      - Example: redis key 'config' will contain the JSON of config.json.
    """
    redis_client.set("config", json.dumps(config))
    logger.info("✅ Stored configuration in Redis.")

def store_last_execution_time(redis_client: redis.Redis, 
                              ) -> str:
    """
    Store the current UTC timestamp in Redis for synchronization.
    
    Returns:
        The stored timestamp in ISO format.
    """
    # .strftime('%Y%m%d%H%M%S')
    last_execution_time = datetime.utcnow()
    redis_client.set("last_crawl_timestamp", last_execution_time.strftime('%Y%m%d%H%M%S'))
    logger.info(f"📌 Stored last execution time: {last_execution_time.strftime('%Y/%m/%d %H:%M:%S')}")

    stored_value = redis_client.get("last_crawl_timestamp")
    logger.info(f"📌 Confirmed last_crawl_timestamp in Redis: {stored_value}")
    return last_execution_time    

def store_keywords_in_redis(redis_client: redis.Redis, 
                            keywords: List[str]
                            ) -> None:
    """
    Update the 'search_keywords' set in Redis with the provided keywords.
    
    Clears existing keywords before updating.
    """
    redis_key = "search_keywords"
    redis_client.delete(redis_key)
    for keyword in keywords:
        redis_client.sadd(redis_key, keyword)
    logger.info(f"🔑 Stored {len(keywords)} search keywords in Redis: {keywords}")