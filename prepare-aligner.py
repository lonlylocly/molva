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

def main():
    logging.info("start")
    parser = util.get_dates_range_parser()
    args = parser.parse_args()

    ind = Indexer(DB_DIR)
    cur = stats.get_cursor(DB_DIR + "/tweets.db")

    day_ago, today = sorted(ind.dates_dbs.keys())[-2:]

    cur1 = ind.get_db_for_date(today)
    cur2 = ind.get_db_for_date(day_ago)

    for table in ["lemma_word_pairs", "tweets_words"]:
        cur.execute("drop table if exists %s" % table)
        for c in [cur, cur1, cur2]:
            stats.create_given_tables(c, [table])

    cur.execute("attach '%s' as day_ago" % ind.dates_dbs[day_ago]) 
    cur.execute("attach '%s' as today" % ind.dates_dbs[today]) 
    cur.execute("attach ':memory:' as tmp") 

    cur.execute("create table tmp.lwp as select * from lemma_word_pairs limit 0")
    logging.info("get united lemma_word_pairs")
    cur.execute("""
        insert into tmp.lwp
        select * from day_ago.lemma_word_pairs
    """)
    cur.execute("""
        insert into tmp.lwp 
        select * from today.lemma_word_pairs
    """)
    cur.execute("""
        insert into lemma_word_pairs 
        select noun1_md5, noun2_md5, source1_md5, source2_md5, sum(cnt) from tmp.lwp
        group by noun1_md5, noun2_md5, source1_md5, source2_md5
    """)

    logging.info("done")
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

if __name__ == '__main__':
    main()


