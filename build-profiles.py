#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys
import os
import logging, logging.config

import stats
from Indexer import Indexer
from util import digest
import util

logging.config.fileConfig("logging.conf")

POST_MIN_FREQ = 10

DB_DIR = os.environ["MOLVA_DIR"]

BLOCKED_NOUNS_LIST = u"\n".join(list(u"абвгдеёжзиклмнопрстуфхцчшщыьъэюя"))

BLOCKED_NOUNS = ",".join(map( lambda x: str(digest(x)), BLOCKED_NOUNS_LIST.split("\n")))

NOUNS_LIMIT = 2000

def setup_noun_profiles(cur, tweets_nouns, nouns, post_min_freq = POST_MIN_FREQ):
    profiles_dict = stats.get_noun_profiles(cur, post_min_freq, BLOCKED_NOUNS)

    #stats.set_noun_profiles_tweet_ids(profiles_dict, tweets_nouns)
    logging.info("Profiles len: %s" % len(profiles_dict))
    if len(profiles_dict) > NOUNS_LIMIT:
        short_profiles_dict = {}
        
        for k in sorted(profiles_dict.keys(), key=lambda x: profiles_dict[x].post_cnt, reverse=True)[:NOUNS_LIMIT]:
            short_profiles_dict[k] = profiles_dict[k]

        profiles_dict = short_profiles_dict

        logging.info("Short-list profiles len: %s" % len(profiles_dict))

    stats.set_noun_profiles_total(cur, profiles_dict, post_min_freq, BLOCKED_NOUNS)

    stats.weight_profiles_with_entropy(profiles_dict, nouns) 
   
    return profiles_dict

def save_sims(cur, sims):
    cur.execute("begin transaction")

    for s in sims:
        cur.execute("replace into noun_similarity values (?, ?, ?)", s)

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

        if args.clear:
            logging.info("Clearing previous stats")
            cur.execute("delete from noun_similarity")

        nouns = stats.get_nouns(cur)
        logging.info("nouns len %s" % len(nouns))

        tweets_nouns = {} # stats.get_tweets_nouns(cur)

        profiles_dict = setup_noun_profiles(cur, tweets_nouns, nouns)
        logging.info("profiles len %s" % len(profiles_dict))

        fill_sims(cur, profiles_dict, nouns, tweets_nouns)
        
if __name__ == '__main__':
    main()


