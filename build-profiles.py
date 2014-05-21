#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys
import os
import logging, logging.config
import json

import stats
from Indexer import Indexer
from util import digest
import util

logging.config.fileConfig("logging.conf")

POST_MIN_FREQ = 10

settings = {} 
try:
    settings = json.load(open('global-settings.json', 'r'))
except Exception as e:
    logging.warn(e)

DB_DIR = settings["db_dir"] if "db_dir" in settings else os.environ["MOLVA_DIR"]

BLOCKED_NOUNS_LIST = u"\n".join(list(u"абвгдеёжзиклмнопрстуфхцчшщыьъэюя"))

BLOCKED_NOUNS = ",".join(map( lambda x: str(digest(x)), BLOCKED_NOUNS_LIST.split("\n")))

NOUNS_LIMIT = 2000


def save_sims(cur, sims):
    cur.execute("begin transaction")

    for s in sims:
        cur.execute("replace into noun_sim_new values (?, ?, ?)", s)

    cur.execute("commit")

def fill_sims(cur, profiles_dict, nouns, tweets_nouns):
    logging.info("Start filling sims iteration")
    posts = profiles_dict.keys()

    cnt = 0
    long_cnt = 1
    sims = []
    for i in xrange(0, len(posts)):
        post1 = posts[i]
        for j in xrange(0, len(posts)):
            post2 = posts[j]
            if post1 <= post2:
                continue
            p_compare = profiles_dict[post1].compare_with(profiles_dict[post2])
            sims.append((post1, post2, p_compare.sim))

            cnt += 1

            if len(sims) > 10000:
                save_sims(cur, sims)
                sims = []
            if cnt > long_cnt * 10000:
                long_cnt += 1
                logging.info("Another 10k seen")
     
    save_sims(cur, sims)

def update_sims(cur):
    cur.execute("begin transaction")

    cur.execute("drop table if exists noun_sim_old")
    cur.execute("alter table noun_similarity rename to noun_sim_old")
    cur.execute("alter table noun_sim_new rename to noun_similarity")
    cur.execute("drop table noun_sim_old")

    cur.execute("commit")

def main():
    parser = util.get_dates_range_parser()
    parser.add_argument("-c", "--clear", action="store_true")
    args = parser.parse_args()

    ind = Indexer(DB_DIR)
   
    for date in sorted(ind.dates_dbs.keys()):
        if args.start is not None and date < args.start:
            continue
        if args.end is not None and date > args.end:
            continue
        cur = ind.get_db_for_date(date)
        try:
            cur.execute("select 1 from post_reply_cnt")
        except Exception as ex:
            logging.info("Skip date %s" % date)
            continue
        
        stats.create_given_tables(cur, ["noun_similarity"])
        cur.execute("create table if not exists noun_sim_new as select * from noun_similarity limit 0")
        cur.execute("delete from noun_sim_new")

        nouns = stats.get_nouns(cur)
        logging.info("nouns len %s" % len(nouns))

        profiles_dict = stats.setup_noun_profiles(cur, {}, nouns, 
        post_min_freq = POST_MIN_FREQ, blocked_nouns = BLOCKED_NOUNS, nouns_limit = NOUNS_LIMIT )
        logging.info("profiles len %s" % len(profiles_dict))

        fill_sims(cur, profiles_dict, nouns, {})

        update_sims(cur)

    logging.info("Done")
        
if __name__ == '__main__':
    main()


