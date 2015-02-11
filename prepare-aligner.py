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

def write_tweets_words(cur):
    logging.info("get united tweets_words")
    cur.execute("""
        insert or ignore into tweets_words
        select * from day_ago.tweets_words
    """)
    cur.execute("""
        insert or ignore into tweets_words
        select * from today.tweets_words
    """)
    logging.info("done")

def delete_if_exists(f):
    if os.path.exists(f):
        os.remove(f)

def main():
    logging.info("start")
    parser = util.get_dates_range_parser()
    args = parser.parse_args()

    ind = Indexer(DB_DIR)
    cur = stats.get_cursor(DB_DIR + "/tweets_lemma.db")

    day_ago, today = sorted(ind.dates_dbs.keys())[-2:]

    cur1 = ind.get_db_for_date(today)
    cur2 = ind.get_db_for_date(day_ago)

    lwp_db_file_final = DB_DIR + "/tweets_lemma_word_pairs.db"

    for table in ["tweets_words"]:
        cur.execute("drop table if exists %s" % table)

    for c in [cur, cur1, cur2]:
        stats.create_given_tables(c, ["tweets_words"])

    cur.execute("attach '%s' as day_ago" % ind.dates_dbs[day_ago]) 
    cur.execute("attach '%s' as today" % ind.dates_dbs[today]) 

    write_tweets_words(cur)

if __name__ == '__main__':
    main()


