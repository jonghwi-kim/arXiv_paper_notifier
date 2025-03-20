# app/crawler.py
import logging
import feedparser
import redis
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch
from config import ELASTIC_HOST, ELASTIC_ID, ELASTIC_PASSWORD, REDIS_HOST, ARXIV_API_BASE_URL

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

es_client = Elasticsearch(ELASTIC_HOST, basic_auth=(ELASTIC_ID, ELASTIC_PASSWORD))
redis_client = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)

def fetch_arxiv_papers(start_date, end_date, category="cs.CL"):
    """
    arXiv에서 특정 기간 동안 논문을 크롤링
    :param start_date: 시작 날짜 (datetime)
    :param end_date: 종료 날짜 (datetime)
    :param category: arXiv 카테고리 (기본값: cs.CL)
    :return: 논문 리스트
    """
    search_query = f"cat:{category}+AND+submittedDate:[{start_date.strftime('%Y%m%d%H%M%S')}+TO+{end_date.strftime('%Y%m%d%H%M%S')}]"
    query = f"{ARXIV_API_BASE_URL}search_query={search_query}&start=0&max_results=1000"
    
    logger.info(f"🔍 Fetching papers from {start_date} to {end_date}")
    return feedparser.parse(query).entries

def store_papers_to_elasticsearch(papers):
    """
    크롤링한 논문을 Elasticsearch에 저장
    :param papers: 논문 리스트
    """
    count = 0
    for paper in papers:
        doc = {
            "id": paper.id.split("/")[-1],
            "title": paper.title,
            "authors": [author.name for author in paper.authors],
            "abstract": paper.summary.replace("\n", " "),
            "published_date": paper.published,
            "link": paper.link
        }
        es_client.index(index="arxiv_papers", id=doc["id"], document=doc)
        count += 1
    
    logger.info(f"✅ Stored {count} papers to Elasticsearch.")
    return count

def crawl_and_store():
    """
    최근 24시간 동안의 논문을 크롤링 후 Elasticsearch에 저장
    """
    end_datetime = datetime.utcnow()
    start_datetime = end_datetime - timedelta(hours=24)

    papers = fetch_arxiv_papers(start_datetime, end_datetime)
    if papers:
        store_papers_to_elasticsearch(papers)
    else:
        logger.warning("⚠️ No papers found for this period.")

if __name__ == "__main__":
    crawl_and_store()