import pytest
from actors.index_manager import IndexManager
import pykka
import os
import shutil

test_dir = '/Users/kostyaev/Projects/test_indices/'

@pytest.fixture(scope="module")
def index_manager(request):
    os.mkdir(test_dir)
    manager = IndexManager.start(index_dir=test_dir)
    index_manager_proxy = manager.proxy()

    def fin():
        index_manager_proxy.stop_all()
        pykka.ActorRegistry.stop_all()
        shutil.rmtree(test_dir)

    request.addfinalizer(fin)
    return index_manager_proxy


def test_insert_to_new_index(index_manager):
    index_manager.insert(index_name="test1", vector=[]).get(timeout=3)
    assert 'test1' in os.listdir(test_dir)
    assert index_manager.get_number_of_records("test1").get(timeout=3) == 1


def test_insert_to_existing_index(index_manager):
    index_manager.insert(index_name="test1", vector=[2]).get(timeout=3)
    index_manager.insert(index_name="test1", vector=[3]).get(timeout=3)
    assert index_manager.get_number_of_records("test1").get(timeout=3) == 3