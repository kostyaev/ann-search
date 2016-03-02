from pyhocon import ConfigFactory

conf = ConfigFactory.parse_file("application.conf")

index_dir = conf.get("index_dir")