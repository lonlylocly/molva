#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys
import os
import logging, logging.config
import json
import codecs
import argparse
import re
from datetime import date, timedelta, datetime
import requests

import molva.stats as stats
import molva.util as util

requests.packages.urllib3.disable_warnings()

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

    def __init__(self, text, tw_id, created_at, username="", tweet_id=""):
        self.text = text
        self.words = []
        self.all_words = []
        self.tw_id = tw_id
        self.created_at = created_at
        self.username = username
        self.tweet_id = tweet_id
        self.html = ""

    def to_json(self):
        j = {
            "text": self.text, 
            "words": self.words, 
            "all_words": self.all_words, 
            "created_at": self.created_at, 
            "username": self.username,
            "created_at_str": datetime.strptime(str(self.created_at) ,"%Y%m%d%H%M%S").strftime("%Y-%m-%d %H:%M:%S"),
            "tweet_id": str(self.tweet_id),
            "embed_html": self.html
        }
            
        return j

    def __str__(self):
        j = self.to_json()

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
    

@util.time_logger
def get_tweets_nouns(cur, tweets):
    cur.execute("""
        select n.id, n.noun_md5
        from tweets_nouns n
        where n.id in (%s)
    """ % ",".join([str(x) for x in tweets.keys()]) )    

    while True:
        r = cur.fetchone()
        if r is None or len(r) < 2:
            break
        tw_id, noun_md5 = r
        if tw_id in tweets:
            tweets[tw_id].all_words.append(noun_md5)

    return tweets

@util.time_logger
def get_related_tweets(cur, words):
    word_md5s = set()
    for w in words:
        if w.word_md5 is not None:
            word_md5s.add(str(w.word_md5))
        if w.lemma_md5 is not None:
            word_md5s.add(str(w.lemma_md5))
    word_md5s = list(word_md5s)

    stats.create_given_tables(cur, ["tweets", "tweets_nouns"])
    
    tweets = {}

    cur.execute("""
        select n.id, n.noun_md5, t.id, t.tw_text, t.created_at, t.username
        from tweets_nouns n
        inner join tweets t
        on n.id = t.id
        where noun_md5 in (%s)
        limit 10000
    """ % ",".join(word_md5s) )

    r = cur.fetchall()

    for l in r:
        tw_id, noun_md5, tw_id, tw_text, created_at, username = l
        if tw_id not in tweets:
            tweets[tw_id] = Tweet(tw_text, tw_id, created_at, username, tw_id)

        tweets[tw_id].words.append(noun_md5)

    get_tweets_nouns(cur, tweets)

    return tweets 

def filter_silly_spam(tw):
    tw_text = {}
    for tw_id in tw:
        tw_text[util.digest(tw[tw_id].text)] = tw_id

    tw2 = {}

    for tw_md5 in tw_text:
        tw_id = tw_text[tw_md5]
        tw2[tw_id] = tw[tw_id]

    return tw2

def lookup_two_days(cur1, cur2, words):

    tw1 = get_related_tweets(cur1, words)
    tw2 = get_related_tweets(cur2, words)

    for tw_id in tw1:
        tw2[tw_id] = tw1[tw_id]

    tw3 = filter_silly_spam(tw2) 

    return tw3
    
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
    # len(tweets[x].words), 
    #for t in sorted(tweets.keys(), key=lambda x: (tweets[x].created_at), reverse=True)[:10]:
    #    print tweets[t].__str__()

    return

#
# remove duplicate tweets (i.e. composed from same set of parsed tokens)
#
def dedup_tweets(tweets, all_words=True):
    dedup_tw = {}
    for tw_id in tweets:
        wordset = tweets[tw_id].all_words if all_words else tweets[tw_id].words
        words_str = [str(x) for x in sorted(wordset)]
        text_md5 = util.digest(",".join(words_str))
        if text_md5 not in dedup_tw:
            dedup_tw[text_md5] = tw_id
        elif tweets[dedup_tw[text_md5]].created_at < tweets[tw_id].created_at:
            dedup_tw[text_md5] = tw_id
    groupped_tw = {}
    for text_md5 in dedup_tw:
        groupped_tw[dedup_tw[text_md5]] = tweets[dedup_tw[text_md5]]

    return groupped_tw 

def get_embed_html(tweet):
    try:
        url="https://twitter.com/%s/status/%s" % (tweet.username, tweet.tweet_id)
        html = requests.get('https://api.twitter.com/1/statuses/oembed.json?url='+url+'&maxwidth=250&conversation=none').json()["html"]
        tweet.html = html
        logging.info("Success with tw id: %s" % tweet.tweet_id)
    except:
        logging.exception("Failed to get embedded tweet")
    return tweet

def put_trend(cl):
    trend = 0.0
    for m in cl["members"]:
        if m["trend"] > trend:
            trend = m["trend"]
    cl["trend"] = trend

def get_relevant_tweets(cur1, cur2, cluster):

    words = map(lambda x: Word(word_md5=x["id"]), cluster["members"])
    print_list(words)
    logging.info(cluster["gen_title"])

    tweets = lookup_two_days(cur1, cur2, words)
    print "total tweets: %s" % len(tweets.keys())

    tw_cnt = {}
    for t in tweets.keys():
        l = len(tweets[t].words)
        if l not in tw_cnt:
            tw_cnt[l] = 0
        tw_cnt[l] += 1

    tweets = dedup_tweets(tweets)
    tweets_density = dedup_tweets(tweets, all_words=False)
    rel_tw_ids = sorted(tweets.keys(), key=lambda x: (len(tweets[x].words), tweets[x].created_at), reverse=True)[:10]
    rel_tw = [tweets[x] for x in rel_tw_ids]

    if len(tweets_density) > 3:
        [get_embed_html(x) for x in rel_tw]

    return {
        "tweets": [x.to_json() for x in rel_tw], 
        "relevance_distribution": tw_cnt, 
        "words": map(lambda x: x.word_md5, words), 
        "tweets_cnt": len(tweets.keys()),
        "members_md5": str(cluster["members_md5"]),
        "density": len(tweets_density)
    }

@util.time_logger
def save_relevant(cur_rel, cluster_date, rel_tweets):
    cur_rel.execute("replace into relevant values (?, ?)" , (cluster_date, json.dumps(rel_tweets)))

def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--dir")
    parser.add_argument("--num")
    parser.add_argument("--clusters")
    parser.add_argument("--clusters-out")

    args = parser.parse_args()

    f_out = codecs.open(args.clusters_out, 'w', encoding="utf8")

    today=date.today().strftime('%Y%m%d')
    ystd=(date.today() - timedelta(1)).strftime('%Y%m%d')

    cl = json.load(codecs.open(args.clusters, 'r', encoding="utf8"))
    
    today_time = (datetime.utcnow()).strftime("%Y%m%d%H%M%S")
    update_time = (datetime.now()).strftime("%Y-%m-%d %H:%M:%S")
    
    cur1 = stats.get_cursor("%s/tweets_%s.db" % (args.dir, today))
    cur2 = stats.get_cursor("%s/tweets_%s.db" % (args.dir, ystd))

    rel_tweets = []
    for x in cl:
        put_trend(x)
    filtered_cl = [x for x in cl if x["trend"] > 0.0]
    logging.info("Filtered out %d of %d (trend > 0.0)" % (len(cl) - len(filtered_cl), len(cl)))
    
    top_cl = sorted(cl, key=lambda x: x["trend"], reverse=True)
    tw_with_embed_cnt = 0
    for cluster in top_cl:
        r = get_relevant_tweets(cur1, cur2, cluster)
        rel_tweets.append(r)
        cluster["topic_density"] = r["density"] 

    logging.info("Have %d topics with tweets embeds out of %d" % (tw_with_embed_cnt, len(top_cl))) 

    cur_rel = stats.get_cursor("%s/tweets_relevant.db" % args.dir) 
    stats.create_given_tables(cur_rel, ["relevant"])
    save_relevant(cur_rel, today_time, rel_tweets)

    final_cl = {"clusters": top_cl, "update_time": update_time, "cluster_id": today_time}
    cl_json = json.dump(final_cl, f_out)
    f_out.close()
   
    return

            
if __name__ == '__main__':
    main()
