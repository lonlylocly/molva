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
    c.execute("create table tmp.lwp_inc_tmp as select * from lemma_word_pairs limit 0")

    for db in ["day_ago", "today"]:
        c.execute("""
            insert into tmp.lwp_inc_tmp (
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
            insert into tmp.lwp_inc_tmp 
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

def _insert_lwp_inc_tmp(c):
    c.execute( """
        insert into tmp.lwp_inc_tmp 
        select * 
        from lemma_word_pairs
    """)

def _delete_lemma_word_pairs(c):
    c.execute("delete from lemma_word_pairs")

def _insert_lemma_word_pairs(c):
    c.execute("create table lwp_new.lemma_word_pairs as select * from tmp.lwp_inc_tmp limit 0")
    c.execute("""
        insert into lwp_new.lemma_word_pairs 
        select noun1_md5, noun2_md5, source1_md5, source2_md5, sum(cnt) from tmp.lwp_inc_tmp 
        group by noun1_md5, noun2_md5, source1_md5, source2_md5
    """)

def _create_index_lemma_word_pairs(c):
    c.execute("create index lwp_new.pk on lemma_word_pairs (noun1_md5, noun2_md5)")

@util.time_logger
def apply_increment(c): 
    c.execute("begin transaction")

    _insert_lwp_inc_tmp(c)
    #_delete_lemma_word_pairs(c)
    _insert_lemma_word_pairs(c)
    _create_index_lemma_word_pairs(c)

    c.execute("commit")

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

    tmp_db_file = DB_DIR + "/tweets_lemma.db.tmp"
    delete_if_exists(tmp_db_file)
    lwp_db_file = DB_DIR + "/tweets_lemma_word_pairs_new.db"
    lwp_db_file_final = DB_DIR + "/tweets_lemma_word_pairs.db"

    for table in ["tweets_words", "lwp_inc_tmp"]:
        cur.execute("drop table if exists %s" % table)

    stats.create_given_tables(cur, ["tweets_words"])   
 
    for c in [cur1, cur2]:
        stats.create_given_tables(c, {"lemma_word_pairs0": "lemma_word_pairs"})
        for table in ["lemma_word_pairs", "tweets_words"]:
            stats.create_given_tables(c, [table])

    cur.execute("attach '%s' as day_ago" % ind.dates_dbs[day_ago]) 
    cur.execute("attach '%s' as today" % ind.dates_dbs[today]) 
    cur.execute("attach '%s' as tmp" % tmp_db_file) 
    cur.execute("attach '%s' as lwp_new" % lwp_db_file) 

    logging.info("get united lemma_word_pairs")

    count_increment(cur) 
    apply_increment(cur) 

    delete_if_exists(tmp_db_file)
    os.rename(lwp_db_file, lwp_db_file_final)

    logging.info("done (get united lemma_word_pairs)")

    write_tweets_words(cur)

if __name__ == '__main__':
    main()


