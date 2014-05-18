#!/usr/bin/python
import sys
import os
import logging, logging.config

import stats
from Indexer import Indexer

logging.config.fileConfig("logging.conf")

DB_DIR = os.environ["MOLVA_DIR"]

def main():
    ind = Indexer(DB_DIR)
   
    for date in sorted(ind.dates_dbs.keys()):
        cur = ind.get_db_for_date(date)
        try:
            cur.execute("select 1 from nouns")
        except Exception as ex:
            logging.info("Skip date %s" % date)
            continue
        
        stats.create_tables(cur)

        if cur.execute("select count(*) from tweet_chains").fetchone()[0] > 0:
            logging.info("Skip date %s" % date)
            continue

        stats.fill_tweet_chains(cur)
        stats.fill_post_reply(cur)

if __name__ == '__main__':
    main()
