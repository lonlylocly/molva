#!/usr/bin/python
# -*- coding: utf-8 -*-
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

def print_cols(arr, per_col=20):
    print "".join(map(lambda x: "%16s" % x, arr))

def main():
    parser = util.get_dates_range_parser()
    args = parser.parse_args()

    logging.info("Start")
    ind = Indexer(DB_DIR)

    grades = (1, 10, 100, 1000)
    data = [["date", "nouns", "tweets", "tweet_chains"] + map(lambda x: "cnt > %s" % x,grades) ]
    print data

    dates = []
    for date in sorted(ind.dates_dbs.keys()):
        if args.start is not None and date < args.start:
            continue
        if args.end is not None and date > args.end:
            continue
        cur = ind.get_db_for_date(date)
        
        tables = cur.execute("SELECT name FROM sqlite_master WHERE type='table' and name = 'tweets_nouns'").fetchall()
        if len(tables) == 0:
            logging.error("No tweets_nouns for date %s" % date)
            continue

        stats.create_given_tables(cur, ["post_cnt"])
        post_cnt = cur.execute("select count(*) from post_cnt").fetchone()[0]
        if post_cnt == 0:
            cur.execute("insert or ignore into post_cnt select noun_md5, count(*) from tweets_nouns group by noun_md5")

        cnt = [date]
        nouns_cnt = cur.execute("select count(*) from nouns").fetchone()[0]
        cnt.append(nouns_cnt)
        tweets = cur.execute("select count(*) from tweets").fetchone()[0]
        cnt.append(tweets if tweets is not None else "~")
        tweet_chains = cur.execute("select count(*) from tweet_chains").fetchone()[0]
        cnt.append(tweet_chains if tweet_chains is not None else "~")

        for i in grades:
            cnti = cur.execute("select count(*) from (select 1 from post_cnt where post_cnt > %s group by post_md5)" % i).fetchone()[0]
            cnt.append("%.2f"% ((cnti + 0.0) / nouns_cnt))

        data.append(cnt)
    
    for row in data:   
        print_cols(row)

if __name__ == '__main__':
    main()


