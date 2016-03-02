import pykka
from os.path import join
from os import listdir
from os.path import isdir
from worker import IndexWorker
import config
from loggers import index_manager_logger as logger


class IndexManager(pykka.ThreadingActor):
    def __init__(self):
        logger.info("Initializing actors")
        super(IndexManager, self).__init__()
        self.dir = config.index_dir
        workers = {}
        for f in listdir(self.dir):
            if isdir(join(self.dir,f)):
                workers[f] = IndexWorker.start(index_dir=join(self.dir,f), actor_urn=f).proxy()
        self.workers = workers


    def find(self, index_name, vector, number):
        return self.workers[index_name].find(vector, number)

    def insert(self, index_name, vector):
        return self.workers[index_name].insert(vector)


    def build_new_indicies(self):
        logger.info("Building new indices")
        for worker in self.workers.values():
            worker.build_index()

    def run_compaction(self):
        logger.info("Running compaction")
        for worker in self.workers.values():
            worker.runCompaction()

    def stop_all(self):
        logger.info("Stopping workers, saving state...")
        for worker in self.workers.values():
            worker.runCompaction()
        pykka.ActorRegistry.stop_all()



