#!/usr/bin/python
# -*- coding: utf-8 -*-
import sqlite3
from subprocess import Popen, PIPE, STDOUT
import sys,codecs
import re
import hashlib
import time
import json
import random

import simdict
from util import digest

sys.stdout = codecs.getwriter('utf8')(sys.stdout)

db = 'more_replys2.db'

def get_nouns(cur):
    res = cur.execute("""
        select noun_md5, noun from nouns
    """ ).fetchall()
    
    nouns = {}
    for r in res:
        nouns[r[0]] = r[1]
   
    return nouns

def get_tweets_nouns(cur):
    print "[%s] fetch tweets_nouns " % (time.ctime())
    res = cur.execute("""
        select id, noun_md5 
        from tweets_nouns    
    """).fetchall()
    
    tweets_nouns = {}
    for r in res:
        i = str(r[0])
        n = r[1]
        if n not in tweets_nouns:
            tweets_nouns[n] = []
        tweets_nouns[n].append(i)

    print "[%s] fetch tweets_nouns (done)" % (time.ctime())

    return tweets_nouns
    

def main():
    con = sqlite3.connect(db)
    con.isolation_level = None
    
    cur = con.cursor()
    nouns = get_nouns(cur)

    f = open("reply-noun-stat.json", "w")

    posts_replys = {}
    tweets_nouns = get_tweets_nouns(cur) 
    cnt = 0
    long_cnt = 0

    print "[%s] Startup" % (time.ctime())
    
    for n in nouns:
        if n not in tweets_nouns:
            print "%d (%s) not in tweets_nouns" % (n, nouns[n])
            continue

        post_ids = tweets_nouns[n]
        
        reply_ids = cur.execute("""
            select id from tweets
            where in_reply_to_id in (%s)
        """ % (",".join(post_ids))).fetchall()
        reply_ids = map(lambda x: str(x[0]), reply_ids)

        reply_nouns=cur.execute("""
            select noun_md5, count(*)
            from tweets_nouns
            where id in (%s)
            group by noun_md5
        """ % (",".join(reply_ids))).fetchall()
    
        posts_replys[n] = {}
        for i in reply_nouns:
            reply = i[0]
            cnt_replys = i[1]
            posts_replys[n][reply] = cnt_replys
        cnt = cnt + 1
        if cnt % 100 == 0:
            print "[%s] done %d nouns" % (time.ctime(), cnt)
            long_cnt += 1
        
    f.write(json.dumps(posts_replys))
    f.close()
    
if __name__ == '__main__':
    main()
