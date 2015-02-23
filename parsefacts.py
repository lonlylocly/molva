#!/usr/bin/python
# -*- coding: utf-8 -*-
import hashlib
import time
import sys,codecs
import os
import os.path
import logging, logging.config
import json
import traceback

import xml.etree.cElementTree as ElementTree

from Indexer import Indexer
import stats
import util

logging.config.fileConfig("logging.conf")

sys.stdout = codecs.getwriter('utf8')(sys.stdout)

CHUNK_SIZE = 10000
KEEP_RATIO = 100

settings = {} 
try:
    settings = json.load(open('global-settings.json', 'r'))
except Exception as e:
    logging.warn(e)

DB_DIR = settings["db_dir"] if "db_dir" in settings else os.environ["MOLVA_DIR"]

def create_tables(cur):
    stats.create_given_tables(cur, ["nouns", "tweets_nouns"])

class FailedSeveralTimesException(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

def try_several_times(f, times, finilizer=None):
    tries = 0
    while tries < times:
        try:
            tries += 1
            logging.info("Starting try #%s" % tries)
            res = f()
            return res
        except Exception as e:
            traceback.print_exc()
            logging.error(e)
            if finilizer is not None:
                finilizer()

    raise FailedSeveralTimesException("")

@util.time_logger
def save_nouns(cur, nouns, table="nouns"):
    f = lambda : _save_nouns(cur, nouns, table)
    try_several_times(f, 3, finilizer=lambda : cur.execute("rollback"))

def _save_nouns(cur, nouns, table="nouns"):
    cur.execute("begin transaction")
    for n in nouns:
        cur.execute("insert or ignore into %s (noun_md5, noun) values (?, ?)" % (table), (util.digest(n), n)) 
    
    cur.execute("commit")

@util.time_logger
def save_tweet_nouns(cur, vals):
    f = lambda : _save_tweet_nouns(cur, vals)
    try_several_times(f, 3, finilizer=lambda : cur.execute("rollback"))

@util.time_logger
def save_word_time_cnt(cur, cur_words, vals):
    f = lambda : _save_word_time_cnt(cur, cur_words, vals)
    try_several_times(f, 3, finilizer=lambda : cur_words.execute("rollback"))

def _save_tweet_nouns(cur, vals):
    cur.execute("begin transaction")

    for v in vals:
        cur.execute("insert or ignore into tweets_nouns (id, noun_md5) values (?, ?)",
            (v[0], v[1]) ) 
        cur.execute("insert or ignore into tweets_words (id, noun_md5, source_md5) values (?, ?, ?)", v) 

    cur.execute("commit")   

def _save_word_time_cnt(cur, cur_words, vals):
    cur_words.execute("begin transaction")

    tweet_ids = set()
    for v in vals:
        tweet_ids.add(v[0])

    tweet_times = {}
    cur.execute("""
        select id, created_at 
        from tweets
        where id in (%s)
    """ % ",".join(map(str, tweet_ids)))
    
    while True:
        res = cur.fetchone()
        if res is None:
            break
        t_id, created_at = res
        t_id = int(t_id)
        tenminute = int(str(created_at)[:11]) # обрезаем до десятков минут
        tweet_times[t_id] = tenminute

    for v in vals:
        cur_words.execute("""
            insert or ignore into word_time_cnt
            (word_md5, tenminute, cnt) 
            values (%s, %s, 0)
        """ % (v[1], tweet_times[int(v[0])])) 

        cur_words.execute("""
            update word_time_cnt
            set cnt = cnt + 1
            where 
            word_md5 = %s 
            and tenminute = %s
        """ % (v[1], tweet_times[int(v[0])]))

    cur_words.execute("commit")

def _sort_part(lemma_word_pairs):
    l = sorted(lemma_word_pairs, key=lambda x: (x[0], x[1]))

    return l

def _filter_part(lemma_word_pairs, keep_ratio):
    l = []
    for i in range(0,len(lemma_word_pairs),keep_ratio):
        l.append(lemma_word_pairs[i])

    return l

def _insert_part(cur, lemma_word_pairs):
    cur.executemany("""
        insert or ignore into lemma_word_pairs (noun1_md5, noun2_md5, source1_md5, source2_md5)
        values (?, ?, ?, ?) 
    """, lemma_word_pairs)


def _update_part(cur, lemma_word_pairs):
    cur.executemany("""
        update lemma_word_pairs set cnt = cnt + 1
        where noun1_md5 = ? and noun2_md5 = ? and source1_md5 = ? and source2_md5 = ?
    """, lemma_word_pairs )

@util.time_logger
def _save_lemma_word_pairs(cur, lemma_word_pairs, db_type="", keep_ratio=1):
    logging.info("save %d lemma pairs, type: %s" % (len(lemma_word_pairs), db_type))
    cur.execute("begin transaction")
    
    #for l in lemma_word_pairs:
    #    logging.debug("INSERT: (%s)," % ",".join(map(str,l)))

    lemma_word_pairs = _filter_part(lemma_word_pairs, keep_ratio)

    _insert_part(cur, lemma_word_pairs)

    _update_part(cur, lemma_word_pairs)
    
    cur.execute("commit")
    logging.info("done")


@util.time_logger
def save_lemma_word_pairs(cur, lemma_word_pairs, db_type="", keep_ratio=1):
    f = lambda : _save_lemma_word_pairs(cur, lemma_word_pairs, db_type, keep_ratio)
    try_several_times(f, 3, finilizer=lambda : cur.execute("rollback"))

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
        self.lead_id = None

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
        fact.lead_id = raw_fact.get('LeadID')

        if prep is not None:
            fact.prep_lemma = prep.get('val').lower()
            fact.prep_id = fields_info[1]
        
        facts.append(fact)

    for lead in elem.findall(".//Lead"):
        lead_id = lead.get('id')
        text = ElementTree.fromstring(lead.get('text').encode('utf8'))
        # <N lemma="">text</N> 
        # lemma - базовая форма. text - текущая, производная
        for f in text.findall('.//N'):
            for fact in facts:
                if fact.lead_id != lead_id:
                    continue
                if f.get(fact.noun_id) is not None:
                    fact.noun = f.text.replace('"','')
        for f in text.findall('.//P'):
            for fact in facts:
                if fact.lead_id != lead_id:
                    continue
                if f.get(fact.prep_id) is not None:
                    fact.prep = f.text.replace('"','')

    return facts
        

def parse_facts_file(tweet_index, facts, date):
    ind = Indexer(DB_DIR)

    cur = ind.get_db_for_date(date) 
    cur_main = stats.get_main_cursor(DB_DIR)
    cur_bigram = stats.get_cursor(DB_DIR + "/tweets_bigram.db")
    cur_words = stats.get_cursor("%s/words_%s.db" % (DB_DIR, date))

    stats.create_given_tables(cur, ["nouns", "tweets_nouns", "tweets_words", "lemma_word_pairs"])
    stats.create_given_tables(cur_words, ["word_time_cnt"])
    stats.create_given_tables(cur_bigram, ["lemma_word_pairs"])
    stats.create_given_tables(cur, {"sources": "nouns"})
    stats.create_given_tables(cur_main, ["nouns"])
    stats.create_given_tables(cur_main, {"sources": "nouns"})

    logging.info("Parse index: %s; facts: %s" % (tweet_index, facts))

    ids = open(tweet_index, 'r').read().split("\n")

    logging.info("Got tweet %s ids" % (len(ids)))

    tree = ElementTree.iterparse(facts, events = ('start', 'end'))

    # set larger cache, default 2000 * 1024, this 102400*1024
    #cur_bigram.execute("pragma cache_size = -102400") 

    nouns_total = set()
    sources_total = set()
    noun_sources = []
    tweets_nouns = []
    lemma_word_pairs = []

    for event, elem in tree:
        if event == 'end' and elem.tag == 'document':
            cur_doc = int(elem.attrib['di'])
            post_id = ids[cur_doc -1]
            nouns_preps = get_nouns_preps(elem)
            lemmas = []
            nouns = []
            for np in nouns_preps:
                try:         
                    lemmas.append(util.digest(np.with_prep()))
                    nouns.append(util.digest(np.noun_lemma))
                    nouns_total.add(np.noun_lemma)
                    sources_total.add(np.with_prep())

                    noun_sources.append((post_id, util.digest(np.noun_lemma), util.digest(np.with_prep())))
                    # tweets_nouns.append((post_id, util.digest(np.noun_lemma)))
                except Exception as e:
                    traceback.print_exc()
                    logging.error(e)

            lemma_word_pairs += make_lemma_word_pairs(nouns, lemmas)

            if len(noun_sources) > 10000:
                logging.info("seen %s docid" % (cur_doc))
                save_tweet_nouns(cur, noun_sources)
                save_word_time_cnt(cur, cur_words, noun_sources)
                noun_sources = []

            if len(lemma_word_pairs) >= CHUNK_SIZE :
                save_lemma_word_pairs(cur_bigram, lemma_word_pairs, db_type='bigram', keep_ratio = KEEP_RATIO) 
                lemma_word_pairs = []
               
            elem.clear()

    save_tweet_nouns(cur, noun_sources)
    save_word_time_cnt(cur, cur_words, noun_sources)
    save_lemma_word_pairs(cur_bigram, lemma_word_pairs, db_type='bigram', keep_ratio = KEEP_RATIO) 

    save_nouns(cur, nouns_total)
    save_nouns(cur, sources_total, table="sources")
    save_nouns(cur_main, nouns_total)
    save_nouns(cur_main, sources_total, table="sources")

    return

def rename_file_with_prefix(f, prefix):
    f_name = os.path.basename(f)
    f_dir = os.path.dirname(f)
    f_new = f_dir + "/" + prefix + f_name 
    logging.info("Rename file: '%s' to '%s'" % (f, f_new))
    os.rename(f, f_new)

def main():
    logging.info("Start parsing extracted nouns")

    ind = Indexer(DB_DIR)

    files_for_dates = ind.get_nouns_to_parse()
    for date in sorted(files_for_dates.keys()):
        for t in files_for_dates[date]:
            tweet_index, facts = t 

            try:
                parse_facts_file(tweet_index, facts, date)
                logging.info("Remove index: %s; facts: %s" % (tweet_index, facts))
                os.remove(tweet_index)
                os.remove(facts)

            except Exception as e:
                traceback.print_exc()
                logging.error(e)
                rename_file_with_prefix(tweet_index, "__trash__")
                rename_file_with_prefix(facts, "__trash__")
    
    logging.info("Done")

if __name__ == "__main__":
    main()
