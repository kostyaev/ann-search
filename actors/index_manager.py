import pykka
from os.path import join
from os import listdir
from os.path import isdir
import os
from worker import IndexWorker
from index_builder import IndexBuilder
import config
from loggers import index_manager_logger as logger

class IndexManager(pykka.ThreadingActor):
    def __init__(self, index_dir=config.index_dir, feat_size=128):
        logger.info("Initializing actors")
        super(IndexManager, self).__init__()
        self.dir = index_dir
        workers = {}
        for f in listdir(self.dir):
            if isdir(join(self.dir, f)):
                workers[f] = IndexWorker.start(index_dir=join(self.dir, f), actor_urn=f, feat_size=feat_size).proxy()
        self.workers = workers
        self.feat_size = feat_size
        IndexBuilder.start(feat_size=feat_size)

    def find_nearest(self, index_name, vector, limit):
        if (self.workers.has_key(index_name)):
            return self.workers[index_name].find_nearest(vector, limit).get()
        else:
            return []

    def get_item_by_id(self, index_name, id):
        if (self.workers.has_key(index_name)):
            return self.workers[index_name].get_item_by_id(id).get()
        else:
            return None

    def insert(self, index_name, vector):
        if (self.workers.has_key(index_name)):
            self.workers[index_name].insert(vector)
        else:
            index_dir = join(self.dir, index_name)
            os.mkdir(index_dir)
            self.workers[index_name] = IndexWorker.start(
                    index_dir=index_dir,
                    actor_urn=index_name,
                    feat_size=self.feat_size).proxy()
            logger.info("Created new index {0}".format(index_name))
            self.workers[index_name].insert(vector)

    def get_number_of_records(self, index_name):
        if (self.workers.has_key(index_name)):
            return self.workers[index_name].get_number_of_records().get()
        else:
            return 0

    def build_new_indices(self):
        logger.info("Building new indices")
        for worker in self.workers.values():
            worker.build_index()

    def run_compaction(self):
        logger.info("Running compaction")
        for worker in self.workers.values():
            worker.run_compaction()

    def stop_all(self):
        logger.info("Stopping workers, saving state...")
        for worker in self.workers.values():
            worker.runCompaction()
        pykka.ActorRegistry.stop_all()
