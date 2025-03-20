import redis
import logging
from typing import  Dict, Any
from elasticsearch import Elasticsearch
from config import ELASTIC_HOST, ELASTIC_ID, ELASTIC_PASSWORD, REDIS_HOST

# Redis and Elasticsearch connections
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