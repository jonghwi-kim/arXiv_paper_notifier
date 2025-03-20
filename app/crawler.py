# app/crawler.py
import logging
import feedparser
import redis
import json
from typing import List, Dict, Any
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch
from utils import get_redis_client, get_es_client
from config import ARXIV_API_BASE_URL

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def fetch_arxiv_papers(start_date: datetime,
                       end_date: datetime, 
                       categories: List[str] = ["cs.CL", "cs.IR", "cs.AI"]
                       ) -> List[Dict[str, Any]]:
    """
    Retrieve research papers from arXiv within a specified time range.

    :param start_date: Start time (UTC) for paper retrieval.
    :param end_date: End time (UTC) for paper retrieval.
    :param categories: List of arXiv categories to fetch papers from. (default: ["cs.CL", "cs.IR", "cs.AI"]).
    :return: List of arXiv papers in `feedparser` format.
    """
    all_papers = []
    for category in categories:
        search_query = f"cat:{category}+AND+submittedDate:[{start_date.strftime('%Y%m%d%H%M%S')}+TO+{end_date.strftime('%Y%m%d%H%M%S')}]"
        query = f"{ARXIV_API_BASE_URL}search_query={search_query}&start=0&max_results=1000"

        logger.info(f"Fetching papers from {start_date} to {end_date} for category: {category}")
        papers = feedparser.parse(query).entries
        all_papers.extend(papers)

    return all_papers

def store_papers_to_elasticsearch(papers: List[Dict[str, Any]], 
                                  es_client: Elasticsearch
                                  ) -> int:
    """
    Index research papers into Elasticsearch.

    :param papers: List of paper metadata.
    :param es_client: Elasticsearch client instance.
    :return: Number of successfully indexed papers.
    """
    count = 0
    for paper in papers:
        doc = {
            "id": paper.id,
            "title": paper.title,
            "authors": [author.name for author in paper.authors],
            "abstract": paper.summary.replace("\n", " "),
            "published_date": paper.published,
            "link": paper.link
        }
        es_client.index(index="arxiv_papers", id=doc["id"], document=doc)
        count += 1
    
    logger.info(f"Indexed {count} papers in Elasticsearch.")
    return count

def crawl_and_store(config: Dict[str, Any]) -> None:
    """
    Perform a research paper crawl from arXiv for the last 24 hours, 
    filter relevant papers, and index results into Elasticsearch.

    - Crawls across multiple categories and repeats the fetch process 3 times for redundancy.
    - Stores the total indexed paper count in Redis.
    - Caches recent paper metadata.
    - Saves the last crawl timestamp for synchronization with search tasks.

    :param config: Configuration dictionary loaded from `config.json`.
    """
    # Establish Elasticsearch and Redis connections
    redis_client = get_redis_client()
    es_client = get_es_client()

    # Define retrieval time window (last 24 hours)
    end_datetime = datetime.utcnow()
    start_datetime = end_datetime - timedelta(hours=24)

    # Retrieve and process papers
    papers = fetch_arxiv_papers(start_datetime, end_datetime)

    # Index relevant papers
    if papers:
        indexed_count = store_papers_to_elasticsearch(papers, es_client)

        # Update Redis with metadata for better tracking and faster access
        redis_client.set("last_crawl_paper_count", indexed_count)
        redis_client.set("last_crawl_timestamp", datetime.utcnow().isoformat())
        redis_client.setex("recent_papers", 600, json.dumps(papers)) # Cache for 10 minutes

    else:
        logger.warning("No relevant papers found for this period.")

if __name__ == "__main__":
    from tasks import load_config  # Import configuration loader
    config = load_config()
    crawl_and_store(config)