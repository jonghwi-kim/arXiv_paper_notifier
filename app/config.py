# app/config.py
import os
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()

# 환경 변수 설정
ELASTIC_HOST = os.getenv("ELASTICSEARCH_HOST", "http://localhost:9200")
ELASTIC_ID = os.getenv("ELASTICSEARCH_ID", "elastic")
ELASTIC_PASSWORD = os.getenv("ELASTICSEARCH_PASSWORD")
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
ARXIV_API_BASE_URL = os.getenv("ARXIV_API_BASE_URL", "http://export.arxiv.org/api/query?")
KAKAO_REST_API_KEY = os.getenv("KAKAO_REST_API_KEY")