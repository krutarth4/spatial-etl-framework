import atexit
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta

import apscheduler.events as events
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
from apscheduler.jobstores.redis import RedisJobStore
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.schedulers.background import BackgroundScheduler

from log_manager.logger_manager import LoggerManager
from main_core.core_config import CoreConfig


#
# class JobConfigurationDTO:
#     func: Any
#     trigger = None
#     args = None
#     kwargs = None
#     id = None
#     name = None
#     misfire_grace_time = undefined
#     coalesce = undefined,
#     max_instances = u
#     next_run_time = undefined
#     jobstore = "default",
#     executor = "default",
#     replace_existing = False,

@dataclass
class SchedulerConfDTO:
    name: str
    enable: bool
    description: str
    timezone: str
    # scheduler: str
    scheduler_type: str
    wait_before_shutdown: bool


class InitScheduler:
    _instance_lock = threading.Lock()
    _instance = None

    def __new__(cls, *args, **kwargs):
        """Singleton pattern — only one scheduler running"""
        # Parse the incoming scheduler config
        conf = SchedulerConfDTO(**args[0])

        # If scheduler disabled → DO NOT CREATE INSTANCE
        if not conf.enable:
            LoggerManager("InitScheduler").warning("Scheduler disabled — not creating scheduler instance")
            return None

        if not cls._instance:
            with cls._instance_lock:
                if not cls._instance:
                    # print("creating new class ")
                    cls._instance = super(InitScheduler, cls).__new__(cls)
        # else:
        #     print("class already present ")
        # TODO: Maybe also check the scheduler conf if same or not otherwise shutdown and call new scheduler instance
        return cls._instance

    def __init__(self, scheduler_conf, url: str):
        self.conf_scheduler = SchedulerConfDTO(**scheduler_conf)
        self.logger = LoggerManager(type(self).__name__)
        self.logger.info("Setting up scheduler ....")
        self.executors = {
        "default": ThreadPoolExecutor(10),      # IO tasks
        "process": ProcessPoolExecutor(4),  # CPU tasks
    }
        self.job_defaults = {
            "coalesce": False,
            "max_instances": 3
        }

        # self.jobstores = {
        #     'default': SQLAlchemyJobStore(url, tableschema="test")
        # }
        if not hasattr(self, "scheduler"):
            self.scheduler = BackgroundScheduler(timezone="Europe/Berlin", executors=self.executors)

        self.start_scheduler()
        atexit.register(self.stop_scheduler)

    def start_scheduler(self):
        self.scheduler.start()
        self.logger.info("Scheduler Started")
        self.subscribe_listener()

    def stop_scheduler(self):
        if self.scheduler.running:
            self.logger.error("stopping scheduler gracefully....")
            self.unsubscribe_listener()
            self.scheduler.shutdown(wait=self.conf_scheduler.wait_before_shutdown)
            self.logger.error("Scheduler Stopped")

    def add_job(self, job_conf: dict, job_name: str):
        self.scheduler.add_job(**job_conf, id=job_name, coalesce=True, misfire_grace_time=10)
        self.logger.info("Job added to scheduler")

    @staticmethod
    def my_listener(event):
        if event.exception:
            print('The job crashed :(')
        else:
            print('The job worked :)', event.retval)

    def subscribe_listener(self):
        self.scheduler.add_listener(self.my_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)

    def unsubscribe_listener(self):
        self.scheduler.remove_listener(self.my_listener)

    def get_job_by_id(self, job_name: str):
        return self.scheduler._lookup_job(job_name, "default")

    def run_forever(self):
        try:
            while True:
                time.sleep(10)
        except(KeyboardInterrupt, SystemExit):
            # self.scheduler.remove_all_jobs() # No need as already mentioned
            self.stop_scheduler()


if __name__ == "__main__":
    conf  = CoreConfig().get_value("scheduler")


    def job1():
        print(f"[{datetime.now()}] Job 1 started")
        time.sleep(10)  # simulate long running
        print(f"[{datetime.now()}] Job 1 finished")


    def job2():
        print(f"[{datetime.now()}] Job 2 started")
        time.sleep(5)  # simulate long running
        print(f"[{datetime.now()}] Job 2 finished")
    scheduler = InitScheduler(conf, "")


    run_time = datetime.now() + timedelta(seconds=3)

    # Schedule both jobs to run at EXACT same time
    scheduler.scheduler.add_job(job1, 'date', run_date=run_time)
    scheduler.scheduler.add_job(job2, 'date', run_date=run_time)

    scheduler.run_forever()

