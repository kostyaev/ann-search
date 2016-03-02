from logging import config as _config
import logging

_config.fileConfig('logging.conf')


main_logger = logging.getLogger("Application")
worker_logger = logging.getLogger("Worker")
index_builder_logger = logging.getLogger("IndexBuilder")
index_manager_logger = logging.getLogger("IndexManager")