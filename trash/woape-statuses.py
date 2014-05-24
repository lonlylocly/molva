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
from woape import get_tw_create_time, try_several_times, WoapeException, mark_user_done


logging.config.fileConfig("logging.conf")

log = logging.getLogger('woape-search')

DB_FILENAME = os.environ["MOLVA_DB"] 
CHAINS_GOAL = 1e6
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
    q = "update id_queue set done = 1 where id in (%s) " % ids_enc
    log.debug(q)
    cur.execute(q)

def main_loop_iteration(cur):
    ids = woape.fetch_list(cur, "select id from id_queue where done = 0 limit %d" % PER_QUERY)

    if len(ids) == 0:
        log.info("No ids left")
        return False

    f = lambda :  iteration(cur, ids)
    try_several_times(f, 3, [])
   
    return True



def main():
    start_users = sys.argv[1:]

    log.info("startup")
    cur = stats.get_cursor(DB_FILENAME) 
    woape.create_tables(cur)

    while True:
        res = main_loop_iteration(cur)
        if not res:
            time.sleep(30)
    
if __name__ == "__main__":
    main()
