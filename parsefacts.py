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
import argparse

import xml.etree.cElementTree as ElementTree

from molva.Indexer import Indexer
import molva.stats as stats
import molva.util as util

logging.config.fileConfig("logging.conf")

sys.stdout = codecs.getwriter('utf8')(sys.stdout)

CHUNK_SIZE = 10000
KEEP_RATIO = 100
BAG_SIZE = 5

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
def save_word_time_cnt2(mcur, word_cnt, word_time_cnt_table):
    vals = []
    for i in word_cnt:
        vals.append("(%s, %s, 1)" % i)
    if len(vals) == 0:
        return
    query = """
        INSERT INTO %s 
        (word_md5, tenminute, cnt)
        VALUES %s 
        ON DUPLICATE KEY UPDATE
        cnt = cnt + VALUES(cnt)
    """ % (word_time_cnt_table, ",".join(vals))
    #logging.debug(query[:1000])
    mcur.execute(query)

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

def cut_to_tenminute(event_time):
    tenminute = int(str(event_time)[:11])

    return tenminute

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
        tenminute = cut_to_tenminute(created_at) # обрезаем до десятков минут
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

@util.time_logger
def save_word_mates2(mcur, pairs, table):
    vals = []
    for i in pairs:
        word1, word2, created_at = i
        tenminute = cut_to_tenminute(created_at) 
        if word1 > word2:
            word1, word2 = word2, word1
        vals.append("(%s, %s, %s, 1)" % (word1, word2, tenminute))

    if len(vals) == 0:
        return
    query = """
        INSERT INTO %s
        (word1, word2, tenminute, cnt)
        VALUES %s
        ON DUPLICATE KEY UPDATE
        cnt = cnt + VALUES(cnt)
    """ % (table, ",".join(vals))
    logging.debug(query[:100])
    mcur.execute(query)

@util.time_logger
def save_bigram_day(mcur, bigram_pairs, table):
    logging.info("Bigrams len: %s" % (len(bigram_pairs)))
    vals = []
    for i in bigram_pairs:
        vals.append("(%s, %s, %s, %s, %s, 1)" % i)
    if len(vals) == 0:
        return
    query = """
        INSERT INTO %s
        (word1, word2, source1, source2, tenminute, cnt)
        VALUES %s
        ON DUPLICATE KEY UPDATE
        cnt = cnt + VALUES(cnt)
    """ % ( table, ",".join(vals))
    logging.debug(query[:200])

    mcur.execute(query)


def make_lemma_word_pairs(words, lemmas, tenminute):
    word_pairs = make_word_pairs(words)
    lemma_pairs = make_word_pairs(lemmas)
    lemma_word_pairs = []
    for i in range(0, len(word_pairs)):
        lemma_word_pairs.append((word_pairs[i][0], word_pairs[i][1], lemma_pairs[i][0], lemma_pairs[i][1], tenminute))

    return lemma_word_pairs    

def make_word_pairs(words, bag_size=2):
    pairs = []
    if len(words) < 2:
        return pairs
    for n in range(2, bag_size + 1):
        for i in range(0,len(words) - n - 1):
            word1 = words[i]
            word2 = words[i + n -1]
            pairs.append((word1, word2))

    return pairs

def make_word_pairs_with_time(words, create_time, bag_size):
    pairs = make_word_pairs(words, bag_size)
    with_time = map(lambda x: (x[0], x[1], create_time), pairs)

    return with_time

class SimpleFact:

    def __init__(self):
        self.noun = None
        self.prep = None
        self.noun_lemma = None
        self.prep_lemma = None
        self.noun_id = None
        self.prep_id = None
        self.lead_id = None
        self.is_hash_tag = False
        self.is_person_name = False
        self.is_number = False

    def __str__(self):
        s = json.dumps({
            "noun": self.noun, 
            "noun_id": self.noun_id, 
            "noun_lemma": self.noun_lemma,
            "prep": self.prep, 
            "prep_id": self.prep_id, 
            "prep_lemma": self.prep_lemma,
            "is_hash_tag": self.is_hash_tag
        }, ensure_ascii=False)

        return s

    def with_prep(self):
        return self.noun if self.prep is None else self.prep + " " + self.noun

def get_nouns_preps(elem):
    facts = []
    for raw_fact in elem.findall(".//SimpleFact"):
        fields_info = raw_fact.get('FieldsInfo').split(';')
        noun = raw_fact.find("./Noun").get('val').lower()
        prep = raw_fact.find("./Prep")
        is_hash_tag = raw_fact.find("./IsHashTag")
        is_person_name = raw_fact.find("./IsPersonName")
        is_number = raw_fact.find("./IsNumber")

        fact = SimpleFact()
        fact.noun_lemma = noun
        fact.noun_id = fields_info[0]
        fact.lead_id = raw_fact.get('LeadID')

        if prep is not None:
            fact.prep_lemma = prep.get('val').lower()
            fact.prep_id = fields_info[1]
        
        if is_hash_tag is not None:
            fact.is_hash_tag = str(is_hash_tag.get('val')) == '1'

        if is_person_name is not None:
            fact.is_person_name = str(is_person_name.get('val')) == '1'

        if is_number is not None:
            fact.is_number = str(is_number.get('val')) == '1'

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
                    if fact.is_hash_tag:
                        fact.noun = '#' + fact.noun

        for f in text.findall('.//P'):
            for fact in facts:
                if fact.lead_id != lead_id:
                    continue
                if f.get(fact.prep_id) is not None:
                    fact.prep = f.text.replace('"','')

    return facts

class MatchTypeCnt:

    def __init__(self):
        self.total = 0
        self.hash_tag = 0
        self.person_name = 0
        self.number = 0

    def add_cnt(self, facts):
        for f in facts:
            self.total += 1
            if f.is_hash_tag:
                self.hash_tag += 1
            if f.is_person_name:
                self.person_name += 1
            if f.is_number:
                self.number += 1

    def __str__(self):
        hash_ratio   = None if self.total == 0 else float(self.hash_tag) / self.total
        person_ratio = None if self.total == 0 else float(self.person_name) / self.total
        number_ratio = None if self.total == 0 else float(self.number) / self.total
        s = "MatchTypeCnt: Total cnt: %s; Hash tags: %s; Hash/Total ratio: %.2f; Person names: %s; Person/Total ratio: %.2f; " % (
            self.total, self.hash_tag, hash_ratio, self.person_name, person_ratio
        )
        s += "Numbers: %d; Number ratio: %.2f" % (self.number, number_ratio)
        return s
 
@util.time_logger
def parse_facts_file(tweet_index, facts, date):
    ind = Indexer(DB_DIR)

    cur = ind.get_db_for_date(date) 
    cur_main = stats.get_main_cursor(DB_DIR)
    cur_bigram = stats.get_cursor(DB_DIR + "/tweets_bigram.db")
    cur_words = stats.get_cursor("%s/words_%s.db" % (DB_DIR, date))

    mcur = stats.get_mysql_cursor(settings)
    word_time_cnt_table = "word_time_cnt_%s" % date
    word_mates_table = "word_mates_%s" % date
    bigram_table = "bigram_%s" % date
    stats.create_mysql_tables(mcur, {
        word_time_cnt_table: "word_time_cnt",
        word_mates_table: "word_mates",
        bigram_table: "bigram_day"
    })

    stats.create_given_tables(cur, ["nouns", "tweets_nouns", "tweets_words", "lemma_word_pairs"])
    stats.create_given_tables(cur_bigram, ["lemma_word_pairs"])
    stats.create_given_tables(cur, {"sources": "nouns"})
    stats.create_given_tables(cur_main, ["nouns"])
    stats.create_given_tables(cur_main, {"sources": "nouns"})

    logging.info("Parse index: %s; facts: %s" % (tweet_index, facts))

    ids = []
    for l in open(tweet_index, 'r').read().split("\n"):
        if l is None or l == '':
            break
        tw_id, created_at = l.split("\t")
        ids.append((tw_id, created_at)) 

    logging.info("Got tweet %s ids" % (len(ids)))

    tree = ElementTree.iterparse(facts, events = ('start', 'end'))

    # set larger cache, default 2000 * 1024, this 102400*1024
    #cur_bigram.execute("pragma cache_size = -102400") 

    nouns_total = set()
    sources_total = set()
    noun_sources = []
    tweets_nouns = []
    lemma_word_pairs = []
    word_mates = []
    word_cnt = []

    match_type_cnt = MatchTypeCnt()

    for event, elem in tree:
        if event == 'end' and elem.tag == 'document':
            cur_doc = int(elem.attrib['di'])
            post_id, create_time = ids[cur_doc -1]
            nouns_preps = get_nouns_preps(elem)
            match_type_cnt.add_cnt(nouns_preps)
            lemmas = []
            nouns = []
            for np in nouns_preps:
                try:         
                    lemmas.append(util.digest(np.with_prep()))
                    nouns.append(util.digest(np.noun_lemma))
                    nouns_total.add(np.noun_lemma)
                    sources_total.add(np.with_prep())

                    noun_sources.append((post_id, util.digest(np.noun_lemma), util.digest(np.with_prep())))
                    word_cnt.append((util.digest(np.noun_lemma), cut_to_tenminute(create_time)))
                    # tweets_nouns.append((post_id, util.digest(np.noun_lemma)))
                except Exception as e:
                    traceback.print_exc()
                    logging.error(e)

            lemma_word_pairs += make_lemma_word_pairs(nouns, lemmas, cut_to_tenminute(create_time))
            word_mates += make_word_pairs_with_time(nouns, create_time, bag_size=BAG_SIZE)

            if len(noun_sources) > 10000:
                logging.info("seen %s docid" % (cur_doc))
                save_tweet_nouns(cur, noun_sources)
                save_word_time_cnt2(mcur, word_cnt, word_time_cnt_table)
                noun_sources = []
                word_cnt = []

            if len(lemma_word_pairs) >= CHUNK_SIZE :
                save_bigram_day(mcur, lemma_word_pairs, bigram_table)
                lemma_word_pairs = []

            if len(word_mates) >= CHUNK_SIZE:
                logging.info("save %s word_mates" % len(word_mates))
                save_word_mates2(mcur, word_mates, word_mates_table)
                word_mates = []
               
            elem.clear()

    save_tweet_nouns(cur, noun_sources)
    #save_word_time_cnt(cur, cur_words, noun_sources)
    save_word_time_cnt2(mcur, word_cnt, word_time_cnt_table)
    save_bigram_day(mcur, lemma_word_pairs, bigram_table)
    #save_word_mates2(mcur, word_mates, word_mates_table)

    save_nouns(cur, nouns_total)
    save_nouns(cur, sources_total, table="sources")
    save_nouns(cur_main, nouns_total)
    save_nouns(cur_main, sources_total, table="sources")

    logging.info(str(match_type_cnt))

    return

def rename_file_with_prefix(f, prefix):
    f_name = os.path.basename(f)
    f_dir = os.path.dirname(f)
    f_new = f_dir + "/" + prefix + f_name 
    logging.info("Rename file: '%s' to '%s'" % (f, f_new))
    os.rename(f, f_new)

def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--one-file", action="store_true")
    args = parser.parse_args()

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
                if args.one_file:
                    break

            except Exception as e:
                traceback.print_exc()
                logging.error(e)
                rename_file_with_prefix(tweet_index, "__trash__")
                rename_file_with_prefix(facts, "__trash__")
    
    logging.info("Done")

if __name__ == "__main__":
    main()
