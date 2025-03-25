# app/tasks_notifier.py
import logging
from celery import Celery
from notifier import send_notification

# Initialize Celery for the notifier container.
app = Celery('tasks_notifier', broker='redis://redis:6379/0')
app.conf.task_queues = {"notifier": {}}

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

@app.task(queue="notifier")
def send_notifications(*args, **kwargs) -> None:
    """
    Execute the notification process:
    Retrieve papers from Elasticsearch and send notifications.
    """
    logger.info("🔔 Running send_notifications...")
    send_notification()

# docker exec -it notifier python -m tasks_notifier
# docker restart notifier
if __name__ == "__main__":
    send_notifications()
