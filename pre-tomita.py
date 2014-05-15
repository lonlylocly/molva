#!/usr/bin/python
from Indexer import Indexer

import logging, logging.config

logging.config.fileConfig("logging.conf")

DB_DIR = os.environ["MOLVA_DIR"] 

if __name__ == '__main__':
    #main()
    indexer = Indexer(DB_DIR)
    indexer.round_indexing()
