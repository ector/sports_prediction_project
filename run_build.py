from apscheduler.schedulers.blocking import BlockingScheduler
from build import set_all_scripts_on_fire

sche = BlockingScheduler()


@sche.scheduled_job('interval', minutes=8)
def timed_job():
    set_all_scripts_on_fire()


sche.start()
