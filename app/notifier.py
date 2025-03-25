import os
import redis
import logging
import json
import torch
from datetime import datetime, timedelta
from typing import List, Dict, Any
from elasticsearch import Elasticsearch
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from kakao import KakaoMessage
from utils import get_redis_client, get_es_client

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Use GPU if available, else CPU.
DEVICE = "cuda" if torch.cuda.is_available() else "cpu"
logger.info(f"⚡ Using device: {DEVICE}")

def get_search_keywords(redis_client: redis.Redis, config: Dict[str, Any]) -> List[str]:
    """
    Retrieve search keywords from Redis; if not found, return config keywords.
    
    Args:
        redis_client (redis.Redis): Redis client instance.
        config (Dict[str, Any]): Configuration dictionary.
    
    Returns:
        List[str]: List of search keywords.
    """
    keywords = list(redis_client.smembers("search_keywords"))
    if not keywords and config.get("keywords"):
        keywords = config["keywords"]
    if keywords:
        logger.info(f"Loaded {len(keywords)} keywords: {keywords}")
    else:
        logger.warning("⚠️ No search keywords found.")
    return keywords

def search_papers(es_client: Elasticsearch, 
                  keywords: List[str], 
                  start_datetime: datetime,
                  end_datetime: datetime,
                  top_k: int = 20) -> Dict[str, List[Dict[str, Any]]]:
    """
    Query Elasticsearch for research papers matching each keyword.
    
    Args:
        es_client (Elasticsearch): Elasticsearch client instance.
        keywords (List[str]): List of search keywords.
        start_datetime (datetime): Start of the search window.
        end_datetime (datetime): End of the search window.
        top_k (int, optional): Maximum number of results per keyword. Defaults to 20.
    
    Returns:
        Dict[str, List[Dict[str, Any]]]: Mapping of keywords to lists of matching papers.
    """
    if not keywords:
        logger.warning("⚠️ No search keywords provided.")
        return {}

    notifications = {}
    for keyword in keywords:
        query_body = {
            "query": {
                "bool": {
                    "must": [
                        {"match": {"abstract": keyword}},
                        {"range": {"published_date": {
                            "gte": start_datetime.isoformat(),
                            "lte": end_datetime.isoformat()
                        }}}
                    ]
                }
            },
            "size": top_k
        }
        response = es_client.search(index="arxiv_papers", body=query_body)
        notifications[keyword] = [hit["_source"] for hit in response["hits"]["hits"]]
    return notifications

def rerank_papers(query: str, 
                  papers: List[Dict[str, Any]], 
                  reranker_model: AutoModelForSequenceClassification,
                  reranker_tokenizer: AutoTokenizer) -> List[Dict[str, Any]]:
    """
    Re-rank papers using a transformer-based re-ranking model.
    
    Computes a relevance score for each paper's abstract, filters out papers
    with a score below 0.5, and returns the remaining papers sorted in descending order.
    
    Args:
        query (str): The search query.
        papers (List[Dict[str, Any]]): List of papers to re-rank.
        reranker_model (AutoModelForSequenceClassification): The pre-trained re-ranking model.
        reranker_tokenizer (AutoTokenizer): The tokenizer corresponding to the re-ranking model.
    
    Returns:
        List[Dict[str, Any]]: Re-ranked list of papers with a "reranker_score" key.
    """
    doc_texts = [paper.get("abstract", "") for paper in papers]
    features = reranker_tokenizer([query] * len(doc_texts), doc_texts, 
                                  padding=True, truncation=True, return_tensors="pt").to(DEVICE)
    reranker_model.to(DEVICE)
    reranker_model.eval()
    with torch.no_grad():
        scores = reranker_model(**features).logits
        scores = torch.sigmoid(scores)

    filtered_papers = []
    for i, paper in enumerate(papers):
        score = round(scores[i].item(), 3)
        if score >= 0.5:
            paper["reranker_score"] = score
            filtered_papers.append(paper)

    ranked_papers = sorted(filtered_papers, key=lambda p: p["reranker_score"], reverse=True)
    return ranked_papers

def send_notification() -> None:
    """
    Search for research papers and send notifications via KakaoTalk.
    
    - Loads configuration and keywords from Redis.
    - Uses the last crawl timestamp to define the search window.
    - Applies re-ranking on retrieved papers if a reranker is configured.
    - Sends notifications using the configured KakaoTalk messenger.
    
    Returns:
        None
    """
    redis_client = get_redis_client()
    es_client = get_es_client()
    config = json.loads(redis_client.get("config"))

    # Step 1: Initialize Messenger Service.
    messenger_type = config.get("messenger", "kakao").lower()
    if messenger_type == "kakao":
        messenger = KakaoMessage(config)
    else:
        logger.warning(f"⚠️ Messenger '{messenger_type}' is not supported yet.")
        return

    # Step 2: Load keywords and calculate search window.
    keywords = get_search_keywords(redis_client, config)
    crawl_period = config.get("crawl_period", 24)
    last_crawl_time = redis_client.get("last_crawl_timestamp")
    try:
        end_datetime = datetime.strptime(last_crawl_time, "%Y%m%d%H%M%S")
        start_datetime = end_datetime - timedelta(hours=crawl_period)
    except Exception as e:
        logger.warning(f"Failed to parse last_crawl_timestamp: {e}. Defaulting to 24-hour window.")
        end_datetime = datetime.utcnow()
        start_datetime = end_datetime - timedelta(hours=crawl_period)

    # Step 3: Retrieve papers using BM25 retrieval.
    notifications = search_papers(es_client, keywords, start_datetime, end_datetime)

    # If a reranker model is configured, apply re-ranking.
    reranker = config.get("reranker", None)
    if reranker is not None:
        reranker_tokenizer = AutoTokenizer.from_pretrained(reranker)
        reranker_model = AutoModelForSequenceClassification.from_pretrained(reranker)
    
    # Step 4: For each keyword, re-rank and send notifications.
    for keyword, papers in notifications.items():
        if not papers:
            logger.info(f"🔍 No new papers found for keyword: '{keyword}'")
            continue

        ### 🚀 TODO: Implement Advanced Re-Ranking ###
        # Enhance ranking beyond BM25 using re-ranking models (e.g., MiniLM, MonoT5, RankLLM).
        if reranker is not None:
            ranked_papers = rerank_papers(keyword, papers, reranker_model, reranker_tokenizer)
        else:
            ranked_papers = papers

        # TODO: Implement Sorting Algorithm (Post Re-Ranking)
        #   - Example: Sort by acceptance status, venue, citations, etc.

        # TODO: Implement Summarization for Paper
        #   - Example: Use GPT-4, BART, or T5 to summarize abstracts.

        logger.info(f"🔔 Sending {len(ranked_papers)} notifications for '{keyword}' since {last_crawl_time}.")
        if messenger_type == "kakao":
            response = messenger.send_paper_kakao(config, keyword, ranked_papers)
        else:
            logger.warning(f"⚠️ Messenger '{messenger_type}' is not supported yet.")
            return

        if response.status_code == 200:
            logger.info(f"✅ Successfully sent {messenger_type} notifications for keyword: '{keyword}'")
        else:
            logger.warning(f"❌ Failed to send {messenger_type} notifications for '{keyword}': {response.json()}")

if __name__ == "__main__":
    send_notification()

        