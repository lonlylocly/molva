#!/usr/bin/python
import sys
import os
import logging, logging.config
import json

import stats
from Indexer import Indexer
import util

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
   
    for date in sorted(ind.dates_dbs.keys()):
        if args.start is not None and date < args.start:
            continue
        if args.end is not None and date > args.end:
            continue

        cur = ind.get_db_for_date(date)
        try:
            cur.execute("select 1 from nouns")
        except Exception as ex:
            logging.info("Skip date %s" % date)
            continue
        
        stats.create_tables(cur)

        #if cur.execute("select count(*) from tweet_chains").fetchone()[0] > 0:
        #    logging.info("Skip date %s" % date)
        #    continue

        stats.fill_tweet_chains(cur)
        #stats.fill_post_reply(cur)

if __name__ == '__main__':
    main()
