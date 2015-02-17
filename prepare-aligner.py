#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys
import os
import logging, logging.config
import json
import argparse

import stats
from Indexer import Indexer
from util import digest
import util
import aligner

logging.config.fileConfig("logging.conf")

POST_MIN_FREQ = 10

settings = {} 
try:
    settings = json.load(open('global-settings.json', 'r'))
except Exception as e:
    logging.warn(e)

DB_DIR = settings["db_dir"] if "db_dir" in settings else os.environ["MOLVA_DIR"]

@util.time_logger
def _write_tweets_words(cur, db, nouns):
    cur.execute("""
        insert or ignore into tweets_words_simple
        select noun_md5, source_md5 from %s.tweets_words
        where noun_md5 in (%s)
    """ % (db, ",".join(map(str,nouns))))

@util.time_logger
def _create_index(cur):
    cur.execute("""
        create index if not exists
        nouns_idx on tweets_words_simple (noun_md5)
    """)

@util.time_logger
def write_tweets_words(cur, nouns):
    logging.info("get united tweets_words")

    _write_tweets_words(cur, 'day_ago', nouns)
    _write_tweets_words(cur, 'today', nouns)
    _create_index(cur)
    
    logging.info("done")

def delete_if_exists(f):
    if os.path.exists(f):
        os.remove(f)


def main():
    logging.info("start")
    parser = argparse.ArgumentParser()

    parser.add_argument("--clusters")

    args = parser.parse_args()

    ind = Indexer(DB_DIR)
    db_file = DB_DIR + "/tweets_lemma.db"
    delete_if_exists(db_file)
    cur = stats.get_cursor(db_file)

    day_ago, today = sorted(ind.dates_dbs.keys())[-2:]

    cur.execute("attach '%s' as day_ago" % ind.dates_dbs[day_ago]) 
    cur.execute("attach '%s' as today" % ind.dates_dbs[today]) 

    stats.create_given_tables(cur, ["tweets_words_simple"])
    stats.create_given_tables(cur, {"today.tweets_words": "tweets_words", 
        "day_ago.tweets_words": "tweets_words"})

    cl = json.load(open(args.clusters,'r'))
    
    write_tweets_words(cur, aligner.get_cluster_nouns(cl))

if __name__ == '__main__':
    main()


