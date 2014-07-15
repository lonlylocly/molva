#!/usr/bin/python
# -*- coding: utf-8 -*-
import hashlib
import time
import sys,codecs
import os
import logging, logging.config
import json
import traceback
import HTMLParser

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
            (v[0], v[1]) ) 
        cur.execute("insert or ignore into tweets_words (id, noun_md5, source_md5) values (?, ?, ?)", v) 

    cur.execute("commit")   

def save_word_pairs(cur, lemma_pairs, pairs_table="lemma_pairs"):
    logging.info("save %d lemma pairs" % len(lemma_pairs))
    cur.execute("begin transaction")

    cur.executemany("""
        insert or ignore into %s (noun1_md5, noun2_md5)
        values (?, ?) 
    """ % pairs_table, lemma_pairs)

    for l in lemma_pairs:
        cur.execute("""
            update %s set cnt = cnt + 1
            where noun1_md5 = %s and noun2_md5 = %s
        """ % (pairs_table, l[0], l[1]))
    
    cur.execute("commit")
    logging.info("done")

def make_word_pairs(lemmas):
    lemma_pairs = []
    if len(lemmas) <2:
        return lemma_pairs
    for i in range(0,len(lemmas)-1):
        lemma_pairs.append((lemmas[i], lemmas[i+1]))

    return lemma_pairs

def parse_facts_file(tweet_index, facts, cur, cur_main):
    stats.create_given_tables(cur, ["nouns", "tweets_nouns", "tweets_words", "lemma_pairs"])
    stats.create_given_tables(cur, {"sources": "nouns", "word_pairs": "lemma_pairs"})
    stats.create_given_tables(cur_main, ["nouns"])
    stats.create_given_tables(cur_main, {"sources": "nouns"})

    logging.info("Parse index: %s; facts: %s" % (tweet_index, facts))

    ids = open(tweet_index, 'r').read().split("\n")

    logging.info("Got tweet %s ids" % (len(ids)))

    h = HTMLParser.HTMLParser()

    tree = ElementTree.iterparse(facts, events = ('start', 'end'))

    nouns_total = set()
    sources_total = set()
    noun_sources = []
    lemma_pairs = []
    noun_pairs = []

    for event, elem in tree:
        if event == 'end' and elem.tag == 'document':
            cur_doc = int(elem.attrib['di'])
            post_id = ids[cur_doc -1]
            leads = elem.findall(".//Lead")
            lemmas = []
            nouns = []
            for l in leads:
                text = ElementTree.fromstring(l.get('text').encode('utf8'))
                for f in text.findall('.//N'):
                    source = f.text
                    noun = f.get('lemma').lower()
                    noun_sources.append((post_id, digest(noun), digest(source)))
                    lemmas.append(digest(source))
                    nouns.append(digest(noun))

                    nouns_total.add(noun)
                    sources_total.add(source)

            lemma_pairs += make_word_pairs(lemmas)
            noun_pairs += make_word_pairs(nouns)

            if len(noun_sources) > 10000:
                logging.info("seen %s docid" % (cur_doc))
                save_tweet_nouns(cur, noun_sources)
                noun_sources = []

            if len(lemma_pairs) > 20000:
                save_word_pairs(cur, lemma_pairs)
                lemma_pairs = []
                save_word_pairs(cur, noun_pairs, pairs_table="word_pairs") 
                noun_pairs = []
               
            elem.clear()

    save_tweet_nouns(cur, noun_sources)
    save_word_pairs(cur, lemma_pairs)
    save_word_pairs(cur, noun_pairs, pairs_table="word_pairs") 

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
