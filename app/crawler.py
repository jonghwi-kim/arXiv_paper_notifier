# app/crawler.py
import logging
import feedparser
import redis
import json
from typing import List, Dict, Any
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConflictError
from utils import get_redis_client, get_es_client
from config import ARXIV_API_BASE_URL

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def fetch_arxiv_papers(start_date: datetime,
                       end_date: datetime, 
                       categories: List[str] = ["cs.CL", "cs.IR", "cs.AI"]
                       ) -> List[Dict[str, Any]]:
    """
    Retrieve research papers from arXiv within the specified time range.

    Args:
        start_date (datetime): The start time (UTC) for paper retrieval.
        end_date (datetime): The end time (UTC) for paper retrieval.
        categories (List[str], optional): List of arXiv categories to query.
            Defaults to ["cs.CL", "cs.IR", "cs.AI"].

    Returns:
        List[Dict[str, Any]]: A list of paper entries in the format provided by feedparser.
    """
    all_papers = []
    for category in categories:
        search_query = (
            f"cat:{category}+AND+submittedDate:[{start_date.strftime('%Y%m%d%H%M%S')}+TO+"
            f"{end_date.strftime('%Y%m%d%H%M%S')}]"
        )
        query = f"{ARXIV_API_BASE_URL}search_query={search_query}&start=0&max_results=1000"
        logger.info(f"Fetching papers from {start_date} to {end_date} for category: {category}")
        papers = feedparser.parse(query).entries
        all_papers.extend(papers)
    return all_papers


def store_papers_to_elasticsearch(papers: List[Dict[str, Any]], 
                                  es_client: Elasticsearch) -> int:
    """
    Index research papers into Elasticsearch.

    Args:
        papers (List[Dict[str, Any]]): A list of paper metadata dictionaries.
        es_client (Elasticsearch): An Elasticsearch client instance.

    Returns:
        int: The number of papers successfully indexed.
    """
    count = 0
    for paper in papers:
        doc = {
            "id": paper.id,
            "title": paper.title.replace("\n", " "),
            "authors": [author.name for author in paper.authors],
            "abstract": paper.summary.replace("\n", " "),
            "published_date": paper.published,
            "link": paper.link
        }
        try:
            es_client.index(index="arxiv_papers", id=doc["id"], document=doc, op_type="create")
        except ConflictError:
            logger.info(f"Document with id {doc['id']} already exists. Skipping insert.")
        count += 1

    logger.info(f"Indexed {count} papers in Elasticsearch.")
    return count


def crawl_and_store() -> None:
    """
    Perform an arXiv crawl for the last defined period and index the results in Elasticsearch.

    This function:
      - Connects to Redis and Elasticsearch.
      - Determines the search window based on the last crawl timestamp stored in Redis and a crawl period from the config.
      - Fetches and processes papers across multiple categories.
      - Indexes retrieved papers into Elasticsearch.
      - Caches recent paper metadata in Redis and updates the paper count.

    Returns:
        None
    """
    # Establish connections.
    redis_client = get_redis_client()
    es_client = get_es_client()

    # Load configuration from Redis.
    config = json.loads(redis_client.get("config"))

    # Determine the time window for crawling.
    last_crawl_time = redis_client.get("last_crawl_timestamp")
    if last_crawl_time:
        try:
            end_datetime = datetime.strptime(last_crawl_time, "%Y%m%d%H%M%S")
            start_datetime = end_datetime - timedelta(hours=config["crawl_period"])
        except Exception as e:
            logger.warning(f"Failed to parse last_crawl_timestamp: {e}. Defaulting to crawl_period window.")
            end_datetime = datetime.utcnow()
            start_datetime = end_datetime - timedelta(hours=config["crawl_period"])
    else:
        end_datetime = datetime.utcnow()
        start_datetime = end_datetime - timedelta(hours=config["crawl_period"])

    # Retrieve papers from arXiv.
    papers = fetch_arxiv_papers(start_datetime, end_datetime)

    # Index papers into Elasticsearch and cache results.
    if papers:
        paper_count = store_papers_to_elasticsearch(papers, es_client)
        redis_client.setex("recent_papers", 600, json.dumps(papers))  # Cache for 10 minutes.
    else:
        paper_count = 0  
        logger.warning("No relevant papers found for this period.")

    # Update the paper count in Redis.
    redis_client.set("last_crawl_paper_count", paper_count)

if __name__ == "__main__":
    crawl_and_store()
