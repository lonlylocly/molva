#!/usr/bin/python
import sys
import os
import logging, logging.config
import json

import molva.stats as stats
from molva.Indexer import Indexer
import molva.util as util

logging.config.fileConfig("logging.conf")

settings = {} 
try:
    settings = json.load(open('global-settings.json', 'r'))
except Exception as e:
    logging.warn(e)

DB_DIR = settings["db_dir"] if "db_dir" in settings else os.environ["MOLVA_DIR"]


def main():
    parser = util.get_dates_range_parser()
    args = parser.parse_args()

    ind = Indexer(DB_DIR)
   
    logging.info("Clear words for date %s" % args.start)
    cur = ind.get_db_for_date(args.start)

    #cur.execute("begin transaction")
    cur.execute("delete from tomita_progress")
    cur.execute("delete from tweets_nouns")
    cur.execute("delete from tweets_words")
    #cur.execute("delete from word_pairs")

    #cur.execute("commit")
    logging.info("Done")

if __name__ == '__main__':
    main()
