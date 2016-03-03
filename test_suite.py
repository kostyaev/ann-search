import pytest
from actors.index_manager import IndexManager
import pykka
import os
import shutil
import time

test_dir = '/Users/kostyaev/Projects/test_indices/'

@pytest.fixture(scope="module")
def index_manager(request):
    if os.path.exists(test_dir):
         shutil.rmtree(test_dir)
    os.mkdir(test_dir)
    manager = IndexManager.start(index_dir=test_dir, feat_size=1)
    index_manager_proxy = manager.proxy()

    def fin():
        index_manager_proxy.stop_all()
        pykka.ActorRegistry.stop_all()
        shutil.rmtree(test_dir)

    request.addfinalizer(fin)
    return index_manager_proxy


def test_insert_to_new_index(index_manager):
    index_manager.insert(index_name="test1", vector=[1]).get(timeout=3)
    assert 'test1' in os.listdir(test_dir)
    assert index_manager.get_number_of_records("test1").get(timeout=3) == 1

def test_insert_to_existing_index(index_manager):
    index_manager.insert(index_name="test1", vector=[2]).get(timeout=3)
    index_manager.insert(index_name="test1", vector=[2]).get(timeout=3)
    assert index_manager.get_number_of_records("test1").get(timeout=3) == 3

def test_find_in_existing_index(index_manager):
    ids = index_manager.find_nearest(index_name="test1", vector=[2], limit=2).get()
    assert len(ids) == 2
    item = index_manager.get_item_by_id(index_name="test1", id=ids[0]).get()
    assert cmp(item, [2]) == 0
    assert item[0] == 2

    ids = index_manager.find_nearest(index_name="test1", vector=[1], limit=2).get()
    item = index_manager.get_item_by_id(index_name="test1", id=ids[0]).get()
    assert cmp(item, [1]) == 0


def test_build_index(index_manager):
    index_manager.build_new_indices()
    time.sleep(1)
    assert index_manager.get_number_of_records("test1").get(timeout=3) == 3
    assert "test1_0.ann" in os.listdir(test_dir + "test1")


def test_find_in_after_build(index_manager):
    ids = index_manager.find_nearest(index_name="test1", vector=[2], limit=2).get()
    assert len(ids) == 2
    item = index_manager.get_item_by_id(index_name="test1", id=ids[0]).get()
    assert cmp(item, [2]) == 0
    assert item[0] == 2

    ids = index_manager.find_nearest(index_name="test1", vector=[1], limit=2).get()
    item = index_manager.get_item_by_id(index_name="test1", id=ids[0]).get()
    assert cmp(item, [1]) == 0

def test_insert_more_elements(index_manager):
    index_manager.insert(index_name="test1", vector=[3]).get(timeout=3)
    index_manager.insert(index_name="test1", vector=[4]).get(timeout=3)
    assert index_manager.get_number_of_records("test1").get(timeout=3) == 5

def test_build_index_again(index_manager):
    index_manager.build_new_indices()
    time.sleep(1)
    assert index_manager.get_number_of_records("test1").get(timeout=3) == 5
    assert "test1_1.ann" in os.listdir(test_dir + "test1")


def test_find_in_multiple_index_files(index_manager):
    ids = index_manager.find_nearest(index_name="test1", vector=[4], limit=2).get()
    assert len(ids) == 2
    item = index_manager.get_item_by_id(index_name="test1", id=ids[0]).get()
    assert cmp(item, [4]) == 0
    ids = index_manager.find_nearest(index_name="test1", vector=[3], limit=2).get()
    item = index_manager.get_item_by_id(index_name="test1", id=ids[0]).get()
    assert cmp(item, [3]) == 0


def test_compaction(index_manager):
    assert "test1_1.ann" in os.listdir(test_dir + "test1")
    assert index_manager.run_compaction()
    time.sleep(2)
    assert "test1_0.ann" in os.listdir(test_dir + "test1")
    assert "test1_1.ann" not in os.listdir(test_dir + "test1")
    assert index_manager.get_number_of_records("test1").get(timeout=3) == 5

    ids = index_manager.find_nearest(index_name="test1", vector=[4], limit=2).get()
    assert len(ids) == 2
    item = index_manager.get_item_by_id(index_name="test1", id=ids[0]).get()
    assert cmp(item, [4]) == 0