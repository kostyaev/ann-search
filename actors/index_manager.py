import pykka
from os import walk
from os.path import join
from worker import IndexWorker
import config



class IndexManager(pykka.ThreadingActor):
    def __init__(self):
        super(IndexManager, self).__init__()
        self.dir = config.index_dir
        workers = {}
        for x in walk(self.dir):
            workers[x[0]] = IndexWorker.start(index_dir=join(self.dir, x[0]), actor_urn=x[0]).proxy()
        self.workers = workers


    def find(self, index_name, vector, number):
        return self.workers[index_name].find(vector, number)

    def insert(self, index_name, vector):
        return self.workers[index_name].insert(vector)


    def build_new_indicies(self):
        for worker in self.workers.values():
            worker.build_index()

    def run_compaction(self):
        for worker in self.workers.values():
            worker.runCompaction()

    def stop(self):
        for worker in self.workers.values():
            worker.runCompaction()
        pykka.ActorRegistry.stop_all()



