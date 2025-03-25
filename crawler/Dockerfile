FROM python:3.8-slim

WORKDIR /app

ENV PYTHONPATH="/app"

COPY crawler/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ./app /app

CMD ["python", "-m", "app.crawler"] 
