# app/tasks_scheduler.py
import logging
import json
from celery import Celery
from celery.schedules import crontab
from utils import get_redis_client, load_config, store_config_in_redis, store_keywords_in_redis
from datetime import datetime, timedelta
# Initialize Celery app with Redis as the broker.
app = Celery('tasks_scheduler', broker='redis://redis:6379/0')

# Ensure that tasks from both crawler and notifier modules are registered.
app.conf.imports = ["tasks_crawler"]
app.conf.timezone = "UTC"
app.conf.enable_utc = True

# Initialize Redis client.
redis_client = get_redis_client()

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def schedule_workflow() -> None:
    """
    Schedule the daily workflow (crawler → notifier) based on config.
    
    - Reads configuration from Redis.
    - Calculates target UTC time using config["schedule"] and config["time_diff"].
    - Registers a beat schedule that triggers the crawler’s scheduled_crawl task.
    """
    # Use the global config loaded from Redis
    config_str = redis_client.get("config")
    if not config_str:
        logger.error("No configuration found in Redis. Please run the initialization.")
        return
    config = json.loads(config_str)
    
    hour, minute = map(int, config["schedule"].split(":"))
    new_hour = hour - config["time_diff"] if (hour - config["time_diff"]) >= 0 else hour + 24 - config["time_diff"]
    task_name = f"daily-scheduled-notification-{new_hour}-{minute}"

    app.conf.beat_schedule = {
        task_name: {
            "task": "tasks_crawler.scheduled_crawl",
            "schedule": crontab(minute=minute, hour=new_hour),
        }
    }
    logger.info(f"✅ Scheduled workflow: {task_name} at {new_hour}:{minute}")

# ---------------------------------------------------------------------------
# Initialization Block: Load config, store config & keywords, and schedule workflow.
# This block is executed at startup.
# ---------------------------------------------------------------------------

config = load_config()
store_config_in_redis(redis_client, config)
store_keywords_in_redis(redis_client, config["keywords"])

schedule_workflow()
logger.info("⚙️  Scheduler initialized.")

# docker exec -it celery-beat python -m tasks_scheduler
# docker restart celery-beat
# if __name__ == "__main__":
#     config = load_config()
#     store_config_in_redis(redis_client, config)
#     store_keywords_in_redis(redis_client, config["keywords"])

#     schedule_workflow()
#     logger.info("⚙️  Scheduler initialized.")