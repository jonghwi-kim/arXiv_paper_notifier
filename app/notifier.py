# app/notifier.py
import os
import redis
import logging
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch
from kakao import KakaoMessage
from config import ELASTIC_HOST, ELASTIC_ID, ELASTIC_PASSWORD, REDIS_HOST

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

es_client = Elasticsearch(ELASTIC_HOST, basic_auth=(ELASTIC_ID, ELASTIC_PASSWORD))
redis_client = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)

def get_search_keywords():
    """
    Redis 또는 JSON 파일에서 검색 키워드 로드
    :return: 키워드 리스트
    """
    keywords = list(redis_client.smembers("search_keywords"))
    
    if not keywords and os.path.exists("keywords.txt"):
        with open("keywords.txt", "r") as keyword_file:
            queries = keyword_file.readlines()
            for query in queries:
                keywords.append(query.strip())

    logger.info(f"{len(keywords)}개 키워드 쿼리 접수 ({keywords})")
    return keywords

def send_notification():
    """ Elasticsearch에서 논문을 검색하고, 카카오톡으로 알림 전송 """
    kakao_msg = KakaoMessage()
    keywords = get_search_keywords()
    
    if not keywords:
        logger.warning("⚠️ 검색 키워드가 없습니다.")
        return

    notifications = {}
    yesterday = datetime.utcnow() - timedelta(hours=24)

    for keyword in keywords:
        query_body = {
            "query": {"bool": {"must": [{"match": {"abstract": keyword}}, {"range": {"published_date": {"gte": yesterday}}}]}}
        }
        response = es_client.search(index="arxiv_papers", body=query_body)

        ### To Do List ###
        ### Re-Ranking Module ###
        ### Paper Priority Algorithm ###
        ### Summarization Module ###

        notifications[keyword] = [hit["_source"] for hit in response["hits"]["hits"]]

    for keyword, papers in notifications.items():
        if not papers:
            logger.info(f"🔍 Keyword '{keyword}': No new papers found.")
            continue

        logger.info(f"🔔 Found {len(papers)} papers for '{keyword}' from {yesterday}. Sending notifications...")

        # response = kakao_msg.send_paper_custom_kakao(keyword, papers)
        response = kakao_msg.send_paper_default_kakao(keyword, papers)
        if response.status_code == 200:
            logger.info(f"✅ {keyword} 관련 논문 카카오톡 메시지 전송 성공!")
        else:
            logger.warning(f"❌ {keyword} 관련 논문 카카오톡 메시지 전송 실패: {response.json()}")

if __name__ == "__main__":
    send_notification()