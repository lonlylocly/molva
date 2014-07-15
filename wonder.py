#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys
import os
import logging, logging.config
import json
import re

import stats
from Indexer import Indexer
import util

logging.config.fileConfig("logging.conf")

settings = {} 
try:
    settings = json.load(open('global-settings.json', 'r'))
except Exception as e:
    logging.warn(e)

DB_DIR = settings["db_dir"] if "db_dir" in settings else os.environ["MOLVA_DIR"]

def take_first(x):
    return x[0]

def main():
    parser = util.get_dates_range_parser()
    args = parser.parse_args()
    ind = Indexer(DB_DIR)
    cur = ind.get_db_for_date(args.start)

    cur.execute("""
        select t.tw_text, n.noun 
        from tweets t
        inner join tweets_nouns tn
        on t.id = tn.id
        inner join nouns n
        on tn.noun_md5 = n.noun_md5
        limit 1000
    """)

    tw_nouns = {}
    while True:
        l = cur.fetchone()
        if l is None:
            break
        tw, noun = l
        if tw not in tw_nouns:
            tw_nouns[tw] = []
        tw_nouns[tw].append(noun)

    for t in tw_nouns:
        print "%s\n\t%s" % (t, " ".join(tw_nouns[t]))

def main2():
    parser = util.get_dates_range_parser()
    args = parser.parse_args()

    ind = Indexer(DB_DIR)
   
    cur = ind.get_db_for_date(args.start)

    words_t = u"""
я
у
к
в
по
на
ты
мы
до        
на
она
он
и
да
    """
    
    words = map(lambda x: x.strip(), words_t.split("\n"))
    words = filter(lambda x: len(x) > 0, words)

    tweets = cur.execute("select tw_text from tweets").fetchall()
    tweets = map(take_first, tweets)


    expr = {}
    compiled = {}
    for w in words:
        regex = "^(.*[\.,!? ])?" + w + "([\.,!? ].*)?$"
        compiled[w] = re.compile(regex, re.I)
        expr[w] = 0

    logging.info(json.dumps(expr, ensure_ascii=False, indent=4))

    total = 0
    cnt = 0
    matching = []
    not_matching = []
    for t in tweets:
        t = t.lower()
        t = t.replace("\n", " ")

        total += 1
        match = False
        for w in expr:
            if compiled[w].match(t):
                cnt += 1
                expr[w] += 1
                if not match:
                    matching.append(t)
                match = True
        if not match:
            not_matching.append(t)

    logging.info("Total: %s; maching: %s" % (total, cnt))

    logging.info(json.dumps(expr, ensure_ascii=False, indent=4))

    logging.info("Matching")
    logging.info(json.dumps(matching[:10], ensure_ascii=False, indent=2))
    logging.info("Not matching")
    logging.info(json.dumps(not_matching[:20], ensure_ascii=False, indent=2))
        
    

if __name__ == '__main__':
    main()
