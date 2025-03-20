# app/tasks.py
import logging
import redis
import json
from datetime import datetime, timedelta
from typing import Dict, Any, List
from celery import Celery, chain
from celery.schedules import crontab

from crawler import crawl_and_store
from notifier import send_notification
from utils import get_redis_client, get_es_client

app = Celery('tasks', broker='redis://redis:6379/0')

redis_client = get_redis_client()

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def load_config() -> Dict[str, Any]:
    with open("config.json", "r", encoding="utf-8") as file:
        return json.load(file)
    
def store_last_execution_time() -> None:
    """
    Store the last execution timestamp in Redis to synchronize crawling and searching operations.

    :return: None
    """
    last_execution_time = datetime.utcnow().isoformat()
    redis_client.set("last_crawl_timestamp", last_execution_time)
    logger.info(f"📌 Stored last execution time: {last_execution_time}")

def store_keywords_in_redis(keywords: List[str]) -> None:
    """
    Store keywords from `config.json` into Redis for search operations.

    - Ensures that the `search_keywords` set in Redis contains the latest keywords.
    - Clears outdated keywords before updating.

    :param keywords: List of keywords from the configuration file.
    :return: None
    """
    redis_key = "search_keywords"
    
    # Clear existing keywords in Redis before updating
    redis_client.delete(redis_key)
    
    # Store new keywords
    for keyword in keywords:
        redis_client.sadd(redis_key, keyword)

    logger.info(f"🔑 Stored {len(keywords)} search keywords in Redis: {keywords}")

config = load_config()
store_keywords_in_redis(config["keywords"])

hour, minute = map(int, config["schedule"].split(":"))
new_hour = hour - config["time_diff"] if hour - config["time_diff"] >= 0 else hour + 24 - config["time_diff"]
task_name = f"daily-scheduled-notification-{new_hour}-{minute}"
app.conf.beat_schedule = {
    task_name: {
        'task': 'tasks.scheduled_workflow',
        'schedule': crontab(minute=minute, hour=new_hour), 
    },
}
app.conf.timezone = 'UTC'

@app.task(queue="default")
def crawl_papers(*args, **kwargs) -> None:
    """
    Execute the crawling process, fetching papers from arXiv and storing them in Elasticsearch.

    :param config: Dictionary containing configuration settings.
    :return: None
    """
    logger.info("📄 Running crawl_papers...")
    crawl_and_store(config)
    store_last_execution_time()  # Track the execution time for search operations


@app.task(queue="default")
def send_notifications(*args, **kwargs) -> None:
    """
    Retrieve papers from Elasticsearch and send notifications through the configured messaging platform.

    :param config: Dictionary containing configuration settings.
    :return: None
    """
    logger.info("🔔 Running send_notifications...")
    send_notification(config)

@app.task(queue="default")
def scheduled_workflow(*args, **kwargs) -> None:
    """
    Define a Celery task chain to sequentially execute crawling and notification processes.

    :param config: Dictionary containing configuration settings.
    :return: None
    """
    logger.info("⏳ Executing scheduled workflow: crawl_papers -> send_notifications")
    return chain(crawl_papers.s(), send_notifications.s()).apply_async()

@app.task(queue="default")
def run_now() -> None:
    """
    Execute the crawling and notification workflow immediately (after 1 min), useful for manual triggers.

    :return: None
    """
    scheduled_time = datetime.utcnow() + timedelta(minutes=1)
    hour, minute = scheduled_time.hour, scheduled_time.minute
    logger.info(f"🕒 Running crawler & notifications at {hour}:{minute} UTC.")
    store_last_execution_time()  # Ensure immediate runs also update the execution timestamp
    chain(crawl_papers.s(), send_notifications.s()).apply_async()

# docker exec -it celery-beat python -m tasks
# Entry point for manual execution
if __name__ == "__main__":
    run_now()