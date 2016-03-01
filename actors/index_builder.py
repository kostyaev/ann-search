import pykka
from os.path import join
from os import listdir
from annoy import AnnoyIndex
import operator
import numpy as np



class IndexBuilder(pykka.ThreadingActor):
    def __init__(self, feat_size=128, n_trees=100):
        super(IndexBuilder, self).__init__()
        self.feat_size = feat_size
        self.n_trees = n_trees


    def build(self, index_file, vectors, sender_urn):
        new_index = AnnoyIndex(self.feat_size, metric='euclidean')
        for idx, v in enumerate(vectors):
            new_index.add_item(idx, v)
        new_index.build(self.n_trees)
        new_index.save(index_file)
        new_index.unload()
        pykka.ActorRegistry.get_by_urn(sender_urn).proxy().load()


    def merge_indicies(self, index_file_a, index_file_b, sender_urn):
        index_a = AnnoyIndex(self.feat_size, metric='euclidean')
        index_b = AnnoyIndex(self.feat_size, metric='euclidean')
        new_index = AnnoyIndex(self.feat_size, metric='euclidean')

        index_a.load(index_file_a)
        index_b.load(index_file_b)

        cnt = 0
        for i in range(index_a.get_n_items()):
            new_index.add_item(cnt, index_a.get_item_vector(i))
            cnt += 1

        for i in range(index_b.get_n_items()):
            new_index.add_item(cnt, index_a.get_item_vector(i))
            cnt += 1


        new_index_file = index_file_a + ".merged"

        new_index.save()
        index_a.unload()
        index_b.unload()
        new_index.unload()

        pykka.ActorRegistry.get_by_urn(sender_urn).proxy().complete_compaction(
                new_index_file=new_index_file,
                index_file_a=index_file_a,
                index_file_b=index_file_b
        )

