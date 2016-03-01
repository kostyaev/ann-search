import pykka
from os import walk
from os.path import join
from worker import IndexWorker



class IndexManager(pykka.ThreadingActor):
    def __init__(self, actor_urn, dir):
        super(IndexManager, self).__init__()
        self.actor_urn = actor_urn
        self.dir = dir
        workers = {}
        for x in walk(dir):
            workers[x[0]] = IndexWorker.start(index_dir=join(dir, x[0]), actor_urn=x[0]).proxy()
        self.workers = workers


    def find(self, index_name, vector, number):
        return self.workers[index_name].find(vector, number)

    def insert(self, index_name, vector):
        return self.workers[index_name].insert(vector)


    def build_indicies(self):
        for worker in self.workers.values():
            worker.build_index()

    def run_compaction(self):
        for worker in self.workers.values():
            worker.build_indicies()

    def stop(self):
        pykka.ActorRegistry.stop_all()



