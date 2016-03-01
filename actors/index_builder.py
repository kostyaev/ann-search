import pykka
from os.path import join
from os import listdir
from annoy import AnnoyIndex
import operator
import numpy as np



class IndexBuilder(pykka.ThreadingActor):
    def __init__(self, index_dir, actor_urn, feat_size=128, n_trees=100):
        super(IndexWorker, self).__init__()
        self.actor_urn = actor_urn
        self.index_dir = index_dir
        self.feat_size = feat_size
        self.indexes = []
        self.prev_id = 0
        self.mem_store = {}
        self.tmp_mem_store = {}
        self.n_trees=100


    def build(self, index_file, vectors, sender_urn):
        new_index = AnnoyIndex(self.feat_size, metric='euclidean')
        for idx, v in enumerate(vectors):
            new_index.add_item(idx, v)
        new_index.build(self.n_trees)
        new_index.save(index_file)
        new_index.unload()
        sender_urn.load()


