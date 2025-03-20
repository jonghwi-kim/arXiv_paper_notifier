# app/tasks.py
import logging
import redis
from datetime import datetime, timedelta
from celery import Celery, chain
from celery.schedules import crontab
from crawler import crawl_and_store
from notifier import send_notification

app = Celery('tasks', broker='redis://redis:6379/0')
redis_client = redis.Redis(host="redis", port=6379, decode_responses=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

with open("running_schedule.txt", 'r') as schedule_file:
    schedule = schedule_file.readlines()
    for line in schedule:
        hour, minute = map(int, line.split(":"))
        new_hour = hour - 9 if hour - 9 >= 0 else hour + 13
        task_name = f"daily-scheduled-notification-{new_hour}-{minute}"
        app.conf.beat_schedule = {
            task_name: {
                'task': 'tasks.scheduled_workflow',
                'schedule': crontab(minute=minute, hour=new_hour), 
            },
}
app.conf.timezone = 'UTC'
# scheduled_time = datetime.utcnow() + timedelta(minutes=1)
# hour, minute = scheduled_time.hour, scheduled_time.minute

@app.task(queue="default")
def crawl_papers(*args, **kwargs):
    """arXiv 논문 크롤링 및 Elasticsearch 저장"""
    logger.info("📄 Running crawl_papers...")
    crawl_and_store()

@app.task(queue="default")
def send_notifications(*args, **kwargs):
    """Elasticsearch에서 논문 검색 후 알림 전송"""
    logger.info("🔔 Running send_notifications...")
    send_notification()

@app.task(queue="default")
def scheduled_workflow(*args, **kwargs):
    """크롤링 후 알림을 정해진 시간에 순차적으로 실행"""
    logger.info("⏳ Executing scheduled workflow: crawl_papers -> send_notifications")
    return chain(crawl_papers.s(), send_notifications.s()).apply_async()

@app.task(queue="default")
def run_now():
    scheduled_time = datetime.utcnow() + timedelta(minutes=1)
    hour, minute = scheduled_time.hour, scheduled_time.minute
    logger.info(f"🕒 Run crawler & notifications at {hour}:{minute} UTC.")
    logger.info("⏳ Executing scheduled workflow: crawl_papers -> send_notifications")
    chain(crawl_papers.s(), send_notifications.s()).apply_async()

# docker exec -it celery-beat python -m tasks
if __name__ == "__main__":
    run_now()