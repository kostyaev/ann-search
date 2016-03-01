import signal
import sys
from apscheduler.schedulers.blocking import BlockingScheduler

sched = BlockingScheduler()

@sched.scheduled_job('interval', seconds=1)
def timed_job():
    print('This job is run every three minutes.')

def signal_handler(signal, frame):
        print('You pressed Ctrl+C!')
        sys.exit(0)




signal.signal(signal.SIGINT, signal_handler)
print('Press Ctrl+C')
sched.start()


signal.pause()



