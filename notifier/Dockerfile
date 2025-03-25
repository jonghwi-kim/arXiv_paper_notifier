FROM pytorch/pytorch:2.6.0-cuda12.4-cudnn9-devel

WORKDIR /app

ENV PYTHONPATH="/app"

COPY notifier/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ./app /app

CMD ["celery", "-A", "app.tasks", "worker", "--loglevel=info", "-Q", "notifier"]