#!/usr/bin/python
import stats
import logging
import logging.config
import os
import sys
import json
import util
import random

import woape
from woape import get_tw_create_time, try_several_times, WoapeException, mark_user_done


logging.config.fileConfig("logging.conf")

log = logging.getLogger('woape-search')

DB_FILENAME = os.environ["MOLVA_DB"] 
CHAINS_GOAL = 1e6

def iteration(cur, users):
    q = "+OR+".join(map(lambda x: "@" + x, users))
    q += "&count=100"
    resp = woape.get_path("/1.1/search/tweets.json?q=" + q)

    if resp.status is not 200:
        log.error("Response code: %s %s. Response body: %s"% (resp.status, resp.reason, resp.read() ))

        raise WoapeException('Invalid response')        

    content = resp.read()

    id_queue = []
    conv_partners = [] 
    #conv_starters = []
    cnt = 0
    for t in json.loads(content)["statuses"]:
        if not util.got_russian_letters(t["text"]):
            continue
        if t["in_reply_to_status_id"] is not None :
            id_queue.append(t["in_reply_to_status_id"])    
            
            fellow = t["user"]["screen_name"]
            if fellow is not None:
                conv_partners.append(fellow)
            #fellow2 = t["in_reply_to_screen_name"]
            #if fellow2 is not None:
            #    conv_starters.append(fellow2)

            woape.save_tweet(cur, t)
            cnt += 1

    cur.executemany("""
            INSERT OR IGNORE INTO id_queue (id)
            VALUES (? )
        """, map(lambda x: (x, ), list(set(id_queue))))

    cur.executemany("insert or ignore into users (username) values (?)", map(lambda x: (x, ), list(set(conv_partners))))
    for user in users:
        mark_user_done(cur, user)

    log.info("Saved %s tweets" % cnt)

def main_loop_iteration(cur):
    cnt = 0
    users = woape.fetch_list(cur, "select username from users where user_done = 0 order by reply_cnt desc limit 10")

    if len(users) == 0:
        log.info("No users left")
        return cnt 

    f = lambda :  iteration(cur, users)
    try_several_times(f, 3, [])
   
    return 1 

def find_some_followers(cur):
    users = woape.fetch_list(cur, "select username from users")

    i = random.randint(0, len(users))

    log.info("Find followers for %s" % users[i])
    resp = woape.get_path("/1.1/followers/ids.json?screen_name=" + users[i])
    content = json.loads(resp.read())
    f_ids = content["ids"]
    
    followers = []
    f_ids_short = f_ids[:100]
    while len(f_ids_short)> 0:
        resp = woape.post_path("/1.1/users/lookup.json", {"user_id": ",".join(map(str, f_ids_short))})
        for f in json.loads(resp.read()):
            if f["screen_name"] not in users:
                followers.append((f["screen_name"],))

        if len(f_ids) > 100:
            f_ids = f_ids[100:]
            f_ids_short = f_ids[:100]
        else:
            f_ids_short = []
       
    log.info("Got %d new usernames" % len(followers))

    cur.executemany("insert or ignore into users (username) values (?)", followers)
    


def main():
    start_users = sys.argv[1:]

    log.info("startup")
    cur = stats.get_cursor(DB_FILENAME) 
    woape.create_tables(cur)

    cnt = 1
    for user in start_users:
        cur.execute("replace into users values ('%s', 0, 0) " % user)

    while True:
        res = main_loop_iteration(cur)
        cnt += res
        if (cnt % 10) == 1:
            chains = woape.get_chains(cur)     
            if len(chains) >= CHAINS_GOAL:
                log.info("Got enough chains, breaking")
                return  
            else:
                log.info("Has %s chains, need %s, continue" % (len(chains), CHAINS_GOAL))

        if res == 0:
            find_some_followers(cur)
    
if __name__ == "__main__":
    main()
