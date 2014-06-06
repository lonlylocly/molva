#!/usr/bin/python
import stats
import logging
import logging.config
import os
import sys
import json
import util
import time

import woape
from woape import get_tw_create_time, try_several_times, WoapeException, Fetcher


logging.config.fileConfig("logging.conf")

settings = {} 
try:
    settings = json.load(open('global-settings.json', 'r'))
except Exception as e:
    logging.warn(e)


DB_DIR = settings["db_dir"] if "db_dir" in settings else os.environ["MOLVA_DIR"]

PER_QUERY = 100

def iteration(cur, ids):
    ids_enc = ",".join(map(str, ids))
    resp = woape.post_path('/1.1/statuses/lookup.json', {'id': ids_enc})

    if resp.status is not 200:
        log.error(u"Response code: %s %s. Response body: %s"% (resp.status, resp.reason, resp.read() ))

        raise WoapeException('Invalid response')        

    content = resp.read()

    cont = json.loads(content)
    for t in cont:
        if not util.got_russian_letters(t["text"]):
            continue
        log.info("save: %s" % t["text"]) 
        woape.save_tweet(cur, t)

    cur.execute("update statuses_progress set id_done = 1 where id in (%s) " % ids_enc)

def main_loop_iteration(cur, fetcher):
    ids = woape.fetch_list(cur, "select id from statuses_progress where id_done = 0 limit %d" % PER_QUERY)

    if len(ids) == 0:
        log.info("No ids left")
        return False

    f = lambda :  iteration(cur, ids)
    try_several_times(f, 3, [])
   
    return True

def main():
    parser = util.get_dates_range_parser()
    args = parser.parse_args()

    ind = Indexer(DB_DIR)
   
    fetcher = Fetcher(DB_DIR)

    for date in sorted(ind.dates_dbs.keys()):
        if args.start is not None and date < args.start:
            continue
        if args.end is not None and date > args.end:
            continue

        cur = ind.get_db_for_date(date)
        
        ind.get_new_tweets_for_statuses(date)

        while main_loop_iteration(cur, fetcher):
            pass
    
if __name__ == "__main__":
    main()
