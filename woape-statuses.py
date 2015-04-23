#!/usr/bin/python
import stats
import logging
import logging.config
import os
import sys
import json
import time

from Fetcher import get_tw_create_time, Fetcher
from molva.util import try_several_times
from Indexer import Indexer
from molva.Exceptions import WoapeException


logging.config.fileConfig("logging.conf")

settings = {} 
try:
    settings = json.load(open('streaming-settings.json', 'r'))
except Exception as e:
    logging.warn(e)


headers = settings["headers"]
DB_DIR = settings["db_dir"] if "db_dir" in settings else os.environ["MOLVA_DIR"]

PER_QUERY = 100

def lookup_statuses(cur, fetcher):
    ids = cur.execute("select id from statuses_progress where id_done = 0 limit %d" % PER_QUERY).fetchall()

    if len(ids) == 0:
        logging.info("No ids left")
        return False

    f = lambda : fetcher.lookup_statuses_iteration(cur, map(lambda x: x[0], ids))
    try_several_times(f, 3)
   
    return True

def main():
    parser = util.get_dates_range_parser()
    args = parser.parse_args()

    ind = Indexer(DB_DIR)
   

    for date in sorted(ind.dates_dbs.keys())[-2:]:
        if args.start is not None and date < args.start:
            continue
        if args.end is not None and date > args.end:
            continue

        cur = ind.get_db_for_date(date)
        
        ind.add_new_tweets_for_statuses(date)

        while True:
            fetcher = Fetcher(DB_DIR, headers)
            res = lookup_statuses(cur, fetcher)
            if not res:
                logging.info("Sleep 5s")
                time.sleep(5)
                break
                
    
if __name__ == "__main__":
    main()
