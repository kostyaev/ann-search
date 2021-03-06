import signal
import sys
from apscheduler.schedulers.blocking import BlockingScheduler
from actors import IndexManager
from loggers import main_logger as logger
import pykka

scheduler = BlockingScheduler()
index_manager = IndexManager.start().proxy()

@scheduler.scheduled_job('interval', minutes=30)
def compaction_job():
        index_manager.run_compaction()


@scheduler.scheduled_job('interval', minutes=60)
def build_new_indices():
        index_manager.build_new_indices()


def signal_handler(signal, frame):
        index_manager.stop_all()
        pykka.ActorRegistry.stop_all()
        logger.info('Exiting')
        sys.exit(0)



if __name__ == '__main__':
        signal.signal(signal.SIGINT, signal_handler)
        logger.info('Application started, press Ctrl+C to save state and exit')
        scheduler.start()
        signal.pause()



