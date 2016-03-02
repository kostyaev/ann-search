import signal
import sys
from apscheduler.schedulers.blocking import BlockingScheduler
from actors import IndexManager
from logging import config as _config
import logging

scheduler = BlockingScheduler()
index_manager = IndexManager.start().proxy()
_config.fileConfig('logging.conf')
logger = logging.getLogger("AnnSearcher")

@scheduler.scheduled_job('interval', minutes=30)
def compaction_job():
        index_manager.run_compaction()


@scheduler.scheduled_job('interval', minutes=60)
def build_new_indices():
        index_manager.build_new_indices()


def signal_handler(signal, frame):
        logger.info('You pressed Ctrl+C!')
        sys.exit(0)



if __name__ == '__main__':
        signal.signal(signal.SIGINT, signal_handler)
        logger.info('Application started, press Ctrl+C to save state and exit')
        scheduler.start()
        signal.pause()



