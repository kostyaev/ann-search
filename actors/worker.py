import pykka
from os.path import join
from os import listdir
from annoy import AnnoyIndex
import operator
import numpy as np
from index_builder import IndexBuilder


class IndexWorker(pykka.ThreadingActor):
    def __init__(self, index_dir, actor_urn, feat_size=128):
        super(IndexWorker, self).__init__()
        self.actor_urn = actor_urn
        self.index_dir = index_dir
        self.feat_size = feat_size
        self.indexes = []
        self.prev_id = 0
        self.mem_store = []
        self.tmp_mem_store = []
        self.index_files = []
        self.load()

    def load(self):
        if len(self.indexes) > 0:
            for index in self.indexes:
                index.unload()

        for f in sorted(listdir(self.index_dir)):
            if f.endswith(".ann"):
                self.index_files.append(f)
                index = AnnoyIndex(self.feat_size, metric='euclidean')
                index.load(join(self.index_dir, f))
                self.indexes.append(index)
                self.prev_id += index.get_n_items()
        self.mem_store = self.tmp_mem_store
        self.tmp_mem_store = []

    def distance(self, a, b):
        distances = (np.array(a) - np.array(b)) ** 2
        return distances.sum(axis=0)

    def find(self, vector, number):
        candidates = []
        for index in self.indexes:
            ids, distances = index.get_nns_by_vector(vector, number, include_distances=True)
            candidates.extend(zip(ids, distances))

        in_mem_candidates = [(self.prev_id + k + 1, self.distance(v, vector)) for k, v in enumerate(self.mem_store)]
        candidates.extend(in_mem_candidates)
        ids, distances = zip(*sorted(candidates, key=operator.itemgetter(1)))
        return ids

    def insert(self, vector):
        self.mem_store.append(vector)

    def get_next_index_file_name(self):
        return self.actor_urn + '_' + str(len(self.indexes))

    def build_index(self):
        if len(self.mem_store) > 0:
            self.tmp_mem_store = self.mem_store
            self.mem_store = []
            path = join(self.index_dir,self.get_next_index_file_name())
            pykka.ActorRegistry.get_by_class(IndexBuilder)[0].build(index_file=path, vectors=self.mem_store, sender_urn=self.actor_urn)

    def run_compaction(self):
        if len(self.indexes) > 1 and self.indexes[-1].get_n_items() < 1000000:
            pykka.ActorRegistry.get_by_class(IndexBuilder)[0].merge_indicies(self.index_files[-2], self.index_files[-1])