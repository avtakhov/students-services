from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from views.workers.cleanup_events_worker import cleanup_events_worker


def setup_scheduler(scheduler: AsyncIOScheduler):
    scheduler.add_job(
        cleanup_events_worker,
        CronTrigger(hour=0, minute=0),
        id="cleanup-events-daily",
        replace_existing=True,
        coalesce=True,
        max_instances=1,
        misfire_grace_time=900,
    )
