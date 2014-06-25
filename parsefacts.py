#!/usr/bin/python
# -*- coding: utf-8 -*-
import hashlib
import time
import sys,codecs
import os
import logging, logging.config
import json
import traceback

import xml.etree.cElementTree as ElementTree

from util import digest
from Indexer import Indexer
import stats

logging.config.fileConfig("logging.conf")

sys.stdout = codecs.getwriter('utf8')(sys.stdout)

settings = {} 
try:
    settings = json.load(open('global-settings.json', 'r'))
except Exception as e:
    logging.warn(e)

DB_DIR = settings["db_dir"] if "db_dir" in settings else os.environ["MOLVA_DIR"]

def create_tables(cur):
    stats.create_given_tables(cur, ["nouns", "tweets_nouns"])

def save_nouns(cur, nouns, table="nouns"):
    cur.execute("begin transaction")
    for n in nouns:
        cur.execute("insert or ignore into %s (noun_md5, noun) values (?, ?)" % (table), (digest(n), n)) 
    
    cur.execute("commit")

def save_tweet_nouns(cur, vals):
    cur.execute("begin transaction")

    for v in vals:
        cur.execute("insert or ignore into tweets_nouns (id, noun_md5) values (?, ?)",
            (v[0], digest(v[1])) ) 
        cur.execute("insert or ignore into tweets_words (id, noun_md5, source_md5) values (?, ?, ?)", 
            (v[0], digest(v[1]), digest(v[2])) ) 
        cur.execute("update tweets_words set cnt = cnt + 1 where id = ? and noun_md5 = ? and  source_md5 = ?", 
            (v[0], digest(v[1]), digest(v[2])) )

    cur.execute("commit")   

def parse_facts_file(tweet_index, facts, cur, cur_main):
    create_tables(cur)   
    stats.create_given_tables(cur_main, ["nouns", "tweets_words"])
    stats.create_given_tables(cur_main, {"sources": "nouns"})

    logging.info("Parse index: %s; facts: %s" % (tweet_index, facts))

    ids = open(tweet_index, 'r').read().split("\n")

    logging.info("Got tweet %s ids" % (len(ids)))

    tree = ElementTree.iterparse(facts, events = ('start', 'end'))
    cnt = 1
    nouns_total = set()
    sources_total = set()
    noun_sources = []
    for event, elem in tree:
        if event == 'end':
            if elem.tag == 'document':
                cur_doc = elem.attrib['di']
                facts = elem.findall("//SimpleFact")
                for f in facts:
                    noun = f.find("./Noun")
                    source = f.find("./Source")
                    if len(noun) < 3:
                        continue
                    nouns_total |= noun
                    sources_total |= source
                    noun_sources.append((post_id, noun, source))
                if len(posts_nouns) > 10000:
                    logging.info("seen %s docid" % (cur_doc))
                    save_tweet_nouns(cur, noun_sources)
                    noun_sources = []
               
            elem.clear()

    save_tweet_nouns(cur, noun_sources)

    save_nouns(cur, nouns_total)
    save_nouns(cur, sources_total, table="sources")
    save_nouns(cur_main, nouns_total)
    save_nouns(cur_main, sources_total, table="sources")

    return

class FailedSeveralTimesException(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

def try_several_times(f, times):
    tries = 0
    while tries < times:
        try:
            tries += 1
            res = f()
            return res
        except Exception as e:
            traceback.print_exc()
            logging.error(e)

    raise FailedSeveralTimesException("")

def main():
    logging.info("Start parsing extracted nouns")

    ind = Indexer(DB_DIR)

    cur_main = stats.get_cursor(DB_DIR + "/tweets.db")

    files_for_dates = ind.get_nouns_to_parse()
    for date in sorted(files_for_dates.keys()):
        for t in files_for_dates[date]:
            tweet_index, facts = t 

            f = lambda : parse_facts_file(tweet_index, facts, ind.get_db_for_date(date), cur_main)
            try_several_times(f, 3)

            logging.info("Remove index: %s; facts: %s" % (tweet_index, facts))
            os.remove(tweet_index)
            os.remove(facts)
    
    logging.info("Done")

if __name__ == "__main__":
    main()
