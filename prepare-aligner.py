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

@util.time_logger
def count_increment(c): 
    c.execute("drop table if exists lwp_inc_tmp")
    c.execute("create table lwp_inc_tmp as select * from lemma_word_pairs limit 0")

    for db in ["day_ago", "today"]:
        c.execute("""
            insert into lwp_inc_tmp (
                noun1_md5,
                noun2_md5,
                source1_md5, 
                source2_md5,
                cnt
            ) 
            select 
                noun1_md5,
                noun2_md5,
                source1_md5, 
                source2_md5,
                - cnt
            from %(db)s.lemma_word_pairs0
        """ % {"db": db})
        c.execute("""
            insert into lwp_inc_tmp 
            select * 
            from %(db)s.lemma_word_pairs
        """ % {"db": db})

        c.execute("begin transaction")

        c.execute("delete from %(db)s.lemma_word_pairs0" % {"db": db})
        c.execute("""
            insert into %(db)s.lemma_word_pairs0 
            select * from %(db)s.lemma_word_pairs
        """ % {"db": db})
        
        c.execute("commit")

@util.time_logger
def apply_increment(c): 
    c.execute("begin transaction")

    c.execute( """
        insert into lwp_inc_tmp 
        select * 
        from lemma_word_pairs
    """)
    c.execute("delete from lemma_word_pairs")
    c.execute("""
        insert into lemma_word_pairs 
        select noun1_md5, noun2_md5, source1_md5, source2_md5, sum(cnt) from lwp_inc_tmp 
        group by noun1_md5, noun2_md5, source1_md5, source2_md5
    """)

    c.execute("commit")

def main():
    logging.info("start")
    parser = util.get_dates_range_parser()
    args = parser.parse_args()

    ind = Indexer(DB_DIR)
    cur = stats.get_cursor(DB_DIR + "/tweets_lemma.db")

    day_ago, today = sorted(ind.dates_dbs.keys())[-2:]

    cur1 = ind.get_db_for_date(today)
    cur2 = ind.get_db_for_date(day_ago)

    for table in ["tweets_words", "lwp_inc_tmp"]:
        cur.execute("drop table if exists %s" % table)
    for table in ["lemma_word_pairs", "tweets_words"]:
        for c in [cur, cur1, cur2]:
            stats.create_given_tables(c, [table])

    for c in [cur1, cur2]:
        stats.create_given_tables(c, {"lemma_word_pairs0": "lemma_word_pairs"})

    cur.execute("attach '%s' as day_ago" % ind.dates_dbs[day_ago]) 
    cur.execute("attach '%s' as today" % ind.dates_dbs[today]) 

    logging.info("get united lemma_word_pairs")

    count_increment(cur) 
    apply_increment(cur) 

    logging.info("done (get united lemma_word_pairs)")

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


