import logging
import os
import time
from datetime import timedelta

from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore

from NotUsed.db_conf import DbConf
from main_core.core_config import CoreConfig


class Scheduler:

    def __init__(self, url: str):
        jobstores = {
            'default': SQLAlchemyJobStore(url)
        }
        self.scheduler = BackgroundScheduler(jobstores=jobstores, timezone="Europe/Berlin")


from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from datetime import datetime
import threading


class SchedulerManager:
    _instance_lock = threading.Lock()
    _instance = None

    def __new__(cls, *args, **kwargs):
        """Singleton pattern — only one scheduler running"""
        if not cls._instance:
            with cls._instance_lock:
                if not cls._instance:
                    cls._instance = super(SchedulerManager, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        if not hasattr(self, "scheduler"):
            self.scheduler = BackgroundScheduler(timezone="Europe/Berlin")
            self.scheduler.start()

    def add_job(self, func, name: str, minutes: int = 30):
        """Add a periodic job"""
        trigger = IntervalTrigger(minutes=minutes)
        self.scheduler.add_job(func, trigger, id=name, name=name, replace_existing=True)
        print(f"[Scheduler] Added job '{name}' every {minutes} minutes.")

    def shutdown(self):
        self.scheduler.shutdown(wait=False)
        print("[Scheduler] Shutdown complete.")

    # @staticmethod
    # def log_job_run(name: str, status: str = "completed", message: str = None):
    #     """Log job results to PostgreSQL"""
    #     with SessionLocal() as session:
    #         log_entry = JobLog(job_name=name, status=status, message=message)
    #         session.add(log_entry)
    #         session.commit()


def alarm(time):
    print(f"Alarm! This alarm was scheduled at {time}.")


if __name__ == '__main__':

    core_conf = CoreConfig()
    db_url = DbConf(core_conf.get_value("db2")).get_db_url()
    print(db_url)
    s = Scheduler(db_url)

    sch = s.scheduler
    # sch.add_jobstore("sqlalchemy", url = db_url)
    alarm_time = datetime.now() + timedelta(seconds=10)
    # sch.add_job(alarm, "date", run_date=alarm_time, args=[datetime.now()])
    trigger = IntervalTrigger(seconds=5)
    job1 = sch.add_job(alarm, trigger=trigger, name="interval", args=[datetime.now()], replace_existing=True)
    print("To clear the alarms, delete the example.sqlite file.")
    print("Press Ctrl+{} to exit".format("Break" if os.name == "nt" else "C"))

    logging.basicConfig()
    logging.getLogger('apscheduler').setLevel(logging.DEBUG)
    try:
        sch.start()
        while True:
            time.sleep(20)
    except (KeyboardInterrupt, SystemExit):
        print("keyboard interrupted")
        job1.remove()
        sch.shutdown()
        pass
