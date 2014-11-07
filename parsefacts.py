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

def save_lemma_word_pairs(cur, lemma_word_pairs):
    logging.info("save %d lemma pairs" % len(lemma_word_pairs))
    cur.execute("begin transaction")

    cur.executemany("""
        insert or ignore into lemma_word_pairs (noun1_md5, noun2_md5, source1_md5, source2_md5)
        values (?, ?, ?, ?) 
    """, lemma_word_pairs)

    cur.executemany("""
        update lemma_word_pairs set cnt = cnt + 1
        where noun1_md5 = ? and noun2_md5 = ? and source1_md5 = ? and source2_md5 = ?
    """, lemma_word_pairs )
    
    cur.execute("commit")
    logging.info("done")

def make_lemma_word_pairs(words, lemmas):
    word_pairs = make_word_pairs(words)
    lemma_pairs = make_word_pairs(lemmas)
    lemma_word_pairs = []
    for i in range(0, len(word_pairs)):
        lemma_word_pairs.append((word_pairs[i][0], word_pairs[i][1], lemma_pairs[i][0], lemma_pairs[i][1]))

    return lemma_word_pairs    

def make_word_pairs(lemmas):
    lemma_pairs = []
    if len(lemmas) <2:
        return lemma_pairs
    for i in range(0,len(lemmas)-1):
        lemma_pairs.append((lemmas[i], lemmas[i+1]))

    return lemma_pairs

class SimpleFact:

    def __init__(self):
        self.noun = None
        self.prep = None
        self.noun_lemma = None
        self.prep_lemma = None
        self.noun_id = None
        self.prep_id = None

    def __str__(self):
        s = u"%(prep)s %(noun)s [%(noun_lemma)s %(noun_id)s] [%(prep_lemma)s %(prep_id)s]" %  ({"noun": self.noun, "noun_id": self.noun_id, "noun_lemma": self.noun_lemma,
            "prep": self.prep if self.prep is not None else '', "prep_id": self.prep_id, "prep_lemma": self.prep_lemma})

        return s

    def with_prep(self):
        return self.noun if self.prep is None else self.prep + " " + self.noun

def get_nouns_preps(elem):
    facts = []
    for raw_fact in elem.findall(".//SimpleFact"):
        fields_info = raw_fact.get('FieldsInfo').split(';')
        noun = raw_fact.find("./Noun").get('val').lower()
        prep = raw_fact.find("./Prep")

        fact = SimpleFact()
        fact.noun_lemma = noun
        fact.noun_id = fields_info[0]

        if prep is not None:
            fact.prep_lemma = prep.get('val').lower()
            fact.prep_id = fields_info[1]

        facts.append(fact)

    for lead in elem.findall(".//Lead"):
        text = ElementTree.fromstring(lead.get('text').encode('utf8'))
        # <N lemma="">text</N> 
        # lemma - базовая форма. text - текущая, производная
        for f in text.findall('.//N'):
            for fact in facts:
                if f.get(fact.noun_id) is not None:
                    fact.noun = f.text.replace('"','')
        for f in text.findall('.//P'):
            for fact in facts:
                if f.get(fact.prep_id) is not None:
                    fact.prep = f.text.replace('"','')

    return facts
        

def parse_facts_file(tweet_index, facts, cur, cur_main):
    stats.create_given_tables(cur, ["nouns", "tweets_nouns", "tweets_words", "lemma_word_pairs"])
    stats.create_given_tables(cur, {"sources": "nouns"})
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
    lemma_word_pairs = []

    for event, elem in tree:
        if event == 'end' and elem.tag == 'document':
            cur_doc = int(elem.attrib['di'])
            post_id = ids[cur_doc -1]
            nouns_preps = get_nouns_preps(elem)
            lemmas = []
            nouns = []

            current_np = "empty"
            try:
                for np in nouns_preps:
                    current_np = np
                    lemmas.append(digest(np.with_prep()))
                    nouns.append(digest(np.noun_lemma))
                    nouns_total.add(np.noun_lemma)
                    sources_total.add(np.with_prep())

                    noun_sources.append((post_id, digest(np.noun_lemma), digest(np.with_prep())))

                lemma_word_pairs += make_lemma_word_pairs(nouns, lemmas)

                if len(noun_sources) > 10000:
                    logging.info("seen %s docid" % (cur_doc))
                    save_tweet_nouns(cur, noun_sources)
                    noun_sources = []

                if len(lemma_word_pairs) > 20000:
                    save_lemma_word_pairs(cur, lemma_word_pairs) 
                    lemma_word_pairs = []
            except Exception as e:
                print "Error on docid: %s" % cur_doc
                s = traceback.format_exc(e)              
                print s
 
            elem.clear()

    save_tweet_nouns(cur, noun_sources)
    save_lemma_word_pairs(cur, lemma_word_pairs) 

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

    cur_main = stats.get_main_cursor(DB_DIR)

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
