# app/tasks_crawler.py
import logging
from celery import Celery
from crawler import crawl_and_store
from utils import get_redis_client, load_config, store_config_in_redis, store_last_execution_time, store_keywords_in_redis

# Initialize Celery for the crawler container.
app = Celery('tasks_crawler', broker='redis://redis:6379/0')
app.conf.task_queues = {"crawler": {}}

# Initialize Redis client.
redis_client = get_redis_client()

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

@app.task(queue="crawler")
def crawl_papers(*args, **kwargs) -> None:
    """
    Execute the crawling process:
    Fetch research papers from arXiv and store them in Elasticsearch.
    """
    logger.info("📄 Running crawl_papers...")
    crawl_and_store()

@app.task(queue="crawler")
def scheduled_crawl(*args, **kwargs) -> None:
    """
    Sequentially execute crawling and then trigger notifications.
    """
    timestamp = store_last_execution_time(redis_client)
    logger.info(f"⏳ Scheduled crawl triggered at {timestamp}.")

    crawl_papers.apply_async(queue="crawler")
    app.send_task("tasks_notifier.send_notifications", queue="notifier")

@app.task(queue="crawler")
def run_now(*args, **kwargs) -> None:
    """
    Immediately trigger the crawling process followed by notifications.
    Updates config and keywords in Redis, then runs the workflow.
    """
    config = load_config()
    
    store_config_in_redis(redis_client, config)
    store_keywords_in_redis(redis_client, config["keywords"])

    timestamp = store_last_execution_time(redis_client)
    logger.info(f"⚡ User-triggered run_now at {timestamp}.")

    crawl_papers.apply_async(queue="crawler")
    app.send_task("tasks_notifier.send_notifications", queue="notifier")

# docker exec -it crawler python -m tasks_crawler
# docker restart crawler
if __name__ == "__main__":
    run_now()
