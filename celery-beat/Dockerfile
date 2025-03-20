FROM python:3.9-slim

WORKDIR /app

# PYTHONPATH 설정
ENV PYTHONPATH="/app"

# Celery Beat 전용 라이브러리 설치
COPY celery-beat/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 프로젝트 코드 복사
COPY ./app /app

# Celery 실행 전에 폴더 구조 확인
RUN ls -R /app

# Celery Beat 실행
CMD ["celery", "-A", "tasks", "beat", "--loglevel=info"]
