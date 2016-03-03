import pykka
from os.path import join
from os import listdir
from annoy import AnnoyIndex
import operator
import numpy as np
from index_builder import IndexBuilder
import os
from loggers import worker_logger as logger


class IndexWorker(pykka.ThreadingActor):
    def __init__(self, index_dir, actor_urn, feat_size=128):
        super(IndexWorker, self).__init__(actor_urn)
        self.actor_urn = actor_urn
        self.actor_ref.actor_urn = actor_urn
        self.index_dir = index_dir
        self.feat_size = feat_size
        self.indexes = []
        self.prev_id = -1
        self.mem_store = []
        self.index_files = []
        self.load()

    def load(self):
        self.prev_id = -1
        self.indexes = []
        logger.info("Loading index {0}".format(self.actor_urn))
        for index in self.indexes:
            index.unload()

        for f in sorted(listdir(self.index_dir)):
            if f.endswith(".ann"):
                self.index_files.append(join(self.index_dir,f))
                index = AnnoyIndex(self.feat_size, metric='euclidean')
                index.load(join(self.index_dir, f))
                self.indexes.append(index)
                self.prev_id += index.get_n_items()
            elif f.endswith('saved_state'):
                self.mem_store = np.load(join(self.index_dir, f)).tolist()
        logger.info("Loaded {0} files with total {1} records for index {2}"
                    .format(len(self.indexes), self.prev_id + 1, self.actor_urn))

    def distance(self, a, b):
        distances = (np.array(a) - np.array(b)) ** 2
        return distances.sum(axis=0)

    def find_nearest(self, vector, limit):
        candidates = []
        offset = 0
        for index in self.indexes:
            ids, distances = index.get_nns_by_vector(vector, limit, include_distances=True)
            ids = (np.array(ids) + offset).tolist()
            offset += index.get_n_items()
            candidates.extend(zip(ids, distances))

        in_mem_candidates = [(self.prev_id + k + 1, self.distance(v, vector)) for k, v in enumerate(self.mem_store)]
        candidates.extend(in_mem_candidates)
        if len(candidates) == 0:
            return []
        else:
            ids, distances = zip(*sorted(candidates, key=operator.itemgetter(1)))
            return ids[:limit]

    def get_item_by_id(self, id):
        if id <= self.prev_id:
            for index in self.indexes:
                if id - index.get_n_items() < 0:
                    return index.get_item_vector(id)
                else:
                    id -= index.get_n_items()
        elif id <= self.prev_id + len(self.mem_store):
            return self.mem_store[id - self.prev_id - 1]
        else:
            return None

    def insert(self, vector):
        self.mem_store.append(vector)
        logger.info("Inserted new record")
        logger.info("Items: {0}".format(self.mem_store))

    def get_next_index_file_name(self):
        return self.actor_urn + '_' + str(len(self.indexes))

    def build_index(self):
        if len(self.mem_store) > 0:
            logger.info("Building index {0}".format(self.get_next_index_file_name()))
            path = join(self.index_dir, self.get_next_index_file_name()) + '.ann'
            logger.info("Getting indexer {0}".format(pykka.ActorRegistry.get_by_class(IndexBuilder)))
            pykka.ActorRegistry.get_by_class(IndexBuilder)[0].proxy().build(
                    index_file=path,
                    vectors=self.mem_store,
                    sender_urn=self.actor_urn)
            self.mem_store = []
            logger.info("Sent command to build")

    def run_compaction(self):
        if len(self.indexes) > 1 and self.indexes[-1].get_n_items() < 1000000:
            logger.info("Running compaction for index {0}".format(self.actor_urn))
            pykka.ActorRegistry.get_by_class(IndexBuilder)[0].proxy().merge_indicies(self.index_files[-2], self.index_files[-1], self.actor_urn)

    def complete_compaction(self, new_index_file, index_file_a, index_file_b):
        for index in self.indexes:
            index.unload()
        os.remove(index_file_a)
        os.remove(index_file_b)
        os.rename(new_index_file, index_file_a)
        logger.info("Compaction for index {0} completed".format(self.actor_urn))
        self.load()

    def save(self):
        if len(self.mem_store) > 0:
            logger.info("Dumping memory state for index {0}".format(self.actor_urn))
            persisted_mem_store_file = join(self.index_dir, "saved_state")
            np.save(persisted_mem_store_file, np.array(self.mem_store))


    def get_number_of_records(self):
        return len(self.mem_store) + self.prev_id + 1



