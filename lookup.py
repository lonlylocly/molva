#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys
import os
import logging, logging.config
import json
import codecs
import argparse
import re
from datetime import date, timedelta

import stats
import util


logging.config.fileConfig("logging.conf")

class Word:

    def __init__(self, word=None, word_md5=None, lemma=None, lemma_md5=None):
        self.word = word
        self.word_md5 = word_md5
        self.lemma = lemma
        self.lemma_md5 = lemma_md5
    
    def __str__(self):
        j = {"word": self.word, "word_md5": self.word_md5, "lemma": self.lemma, "lemma_md5": self.lemma_md5}
        #return "word=%s; word_md5=%s; lemma=%s; lemma_md5=%s" % (self.word, self.word_md5, self.lemma, self.lemma_md5)
        return json.dumps(j, indent=2, ensure_ascii=False)

class Tweet:

    def __init__(self, text, tw_id):
        self.text = text
        self.words = []
        self.tw_id = tw_id
    
    def __str__(self):
        j = {"text": self.text, "words": self.words}

        return json.dumps(j, indent=2, ensure_ascii=False)

def get_words_from_query(query):
    tokens = re.split('\s+', query)

    words = []

    for t in tokens:
        w = Word(t)
        w.word_md5 = util.digest(t)
        words.append(w)

    return words

def get_word_source(cur, word):
    cur.execute("""
        select t.noun_md5, s.noun
        from tweets_words t
        inner join sources s
        on t.source_md5 = s.noun_md5
        where t.source_md5 = %s
        limit 1
    """ % (word.word_md5))
    l = cur.fetchone()
    if l is not None:
        word.lemma_md5 = l[0]
        word.lemma = l[1]
        return

    cur.execute("""
        select noun_md5, noun 
        from nouns 
        where noun_md5 = %s
    """ % (word.word_md5))
    l = cur.fetchone()
    if l is not None:
        word.lemma_md5 = l[0]
        word.lemma = l[1]
        return


def fill_lemmas(cur, words):
    for word in words:
        get_word_source(cur, word)
    
     
def print_list(l):
    for i in l:
        logging.debug(i.__str__())
    

def get_related_tweets(cur, words):
    word_md5s = set()
    for w in words:
        if w.word_md5 is not None:
            word_md5s.add(str(w.word_md5))
        if w.lemma_md5 is not None:
            word_md5s.add(str(w.lemma_md5))
    word_md5s = list(word_md5s)

    tweets = {}

    cur.execute("""
        select n.id, n.noun_md5, t.tw_text
        from tweets_nouns n
        inner join tweets t
        on n.id = t.id
        where noun_md5 in (%s)
    """ % ",".join(word_md5s) )

    r = cur.fetchall()

    for l in r:
        tw_id, noun_md5, tw_text = l
        if tw_id not in tweets:
            tweets[tw_id] = Tweet(tw_text, tw_id)

        tweets[tw_id].words.append(noun_md5)
    
    return tweets 

def lookup_two_days(cur1, cur2, words):

    tw1 = get_related_tweets(cur1, words)
    tw2 = get_related_tweets(cur2, words)

    for tw_id in tw1:
        tw2[tw_id] = tw1[tw_id]

    return tw2
    
def main2():
    parser = argparse.ArgumentParser()

    parser.add_argument("--dir")
    parser.add_argument("--query")

    args = parser.parse_args()

    words = get_words_from_query(args.query.decode('utf8'))

    print_list(words)

    if args.db is None:
        print "Need --db" 
        return
    
    cur = stats.get_cursor(args.db)
    
    fill_lemmas(cur, words)

    print_list(words)

    tweets = lookup_two_days(cur, cur, words)
    for t in sorted(tweets.keys(), key=lambda x: len(tweets[x].words), reverse=True)[:10]:
        print tweets[t].__str__()

    return

def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--dir")
    args = parser.parse_args()

    today=date.today().strftime('%Y%m%d')
    ystd=(date.today() - timedelta(1)).strftime('%Y%m%d')


    cur_display = stats.get_cursor("%s/tweets_display.db" % args.dir) 
    cur1 = stats.get_cursor("%s/tweets_%s.db" % (args.dir, today))
    cur2 = stats.get_cursor("%s/tweets_%s.db" % (args.dir, ystd))
   
    cur_display.execute("""
        select cluster_date, cluster from clusters
        order by cluster_date desc
        limit 1
    """) 

    d, c = cur_display.fetchone()
    c = json.loads(c)

    words = map(lambda x: Word(word_md5=x["id"]), c[0]["members"])

    tweets = lookup_two_days(cur, cur, words)
    for t in sorted(tweets.keys(), key=lambda x: len(tweets[x].words), reverse=True)[:10]:
        print tweets[t].__str__()

    return

            
if __name__ == '__main__':
    main()
