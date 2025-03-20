FROM python:3.8-slim

WORKDIR /app

ENV PYTHONPATH="/app"

COPY notifier/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ./app /app

CMD ["celery", "-A", "app.tasks", "worker", "--loglevel=info", "-Q", "notifier"]  # Notifier Celery 워커 실행 (notifier용)
