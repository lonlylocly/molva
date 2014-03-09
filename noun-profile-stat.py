#!/usr/bin/python
# -*- coding: utf-8 -*-
import sqlite3
import sys,codecs
import time

from stats import get_tweets_nouns, get_post_replys_tweets 

sys.stdout = codecs.getwriter('utf8')(sys.stdout)

db = 'more_replys2.db'

POST_MIN_FREQ = 30
REPLY_MIN_FREQ = 20

def get_noun_cnt(cur):
    stats = cur.execute("""
        select post_md5, post_cnt 
        from post_cnt        
    """).fetchall()

    stats_dict = {}
    for s in stats:
        post, cnt = s
        stats_dict[post] = cnt

    return stats_dict 

def set_post_replys_cnt(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS post_cnt
        ( post_md5 integer, reply_cnt integer , PRIMARY KEY(post_md5))
    """)

    tw_n = get_tweets_nouns(cur)

    post_md5s = cur.execute("select distinct post_md5 from post_reply_cnt").fetchall()

    cnt = 0 
    max_cnt = len(post_md5s)
    for post in set(map(lambda x: x[0], post_md5s)):
        replys = get_post_replys_tweets(cur, tw_n, post)
        cur.execute("insert into post_cnt values (?, ?)" , (post, len(replys)))
        print "[%s] done %d of %d " % (time.ctime(), cnt, max_cnt)

def get_noun_profiles(cur):
    stats = cur.execute("""
        select post_md5, reply_md5, reply_cnt
        from post_reply_cnt
        where post_cnt > %d
        and reply_cnt > %d
    """ % (POST_MIN_FREQ, REPLY_MIN_FREQ )).fetchall()

    stats_dict = {}

    for s in stats:
        post = int(s[0])
        reply = int(s[1])
        cnt = int(s[2])
        if post not in stats_dict:
            stats_dict[post] = {reply: cnt}
        stats_dict[post][reply] = cnt
  
    print "[%s] Lookup noise stats " % (time.ctime())
    noise_stats = cur.execute("""
        select p.post_md5, p.reply_md5, r.reply_id
        from post_reply_cnt p
        inner join post_reply_tweet_chains r
        on p.post_md5 = r.post_md5 and p.reply_md5 = r.reply_md5
        where p.post_cnt > %d
        and p.reply_cnt <= %d
    """ % (POST_MIN_FREQ, REPLY_MIN_FREQ)).fetchall()

    print "[%s] Lookup noise stats (fetch done)" % (time.ctime())

    noise_dict = {} 
    for s in noise_stats:
        post = int(s[0])
        reply = int(s[1])
        reply_id = int(s[2])
        if post not in noise_dict:
            noise_dict[post] = [] 
        noise_dict[post].append(reply_id)

    for post in noise_dict:
        reply_cnt = len(set(noise_dict[post]))
        if post not in stats_dict:
            stats_dict[post] = {}
        stats_dict[post][0] = reply_cnt
   
    print "[%s] Lookup noise stats (done)" % (time.ctime())

    return stats_dict

def get_post_cnt(cur):
    stats = cur.execute("""
        select post_md5, reply_cnt
        from post_cnt
    """).fetchall()

    stats_dict = {}
    for s in stats:
        post = int(s[0])
        reply_cnt = int(s[1])
        stats_dict[post] = reply_cnt

    return stats_dict

def print_stats_row(key, row):       
    print key 
    ok_cnt = 0
    noise_cnt = 0
    for reply in row:
        print "\t%s\t%s" % (reply, row[reply])
        if reply == 0:
            noise_cnt += row[reply]
        else:
            ok_cnt += row[reply]

    print "\ttotal ok: %s" % ok_cnt
    print "\ttotal nouse: %s" % noise_cnt
    print "\tsum: %s" % (ok_cnt + noise_cnt)

    
 
def get_rel_stats(abs_stats, post_reply_cnt):
    rel_stats = {}
    for post in abs_stats:
        replys_cnt = post_reply_cnt[post]
        rel_stats[post] = {}
        for reply in abs_stats[post]:
            repl_portion = (abs_stats[post][reply] + 0.0)/ replys_cnt
            rel_stats[post][reply] = repl_portion
        print_stats_row(post, rel_stats[post])

    return rel_stats

def main():
    print "[%s] Startup " % (time.ctime())
    con = sqlite3.connect(db)
    con.isolation_level = None
    
    cur = con.cursor()

    abs_stats = get_noun_profiles(cur)
    post_cnt = get_post_cnt(cur)
    rel_stats = get_rel_stats(abs_stats, post_cnt)

    
    print "[%s] Done " % (time.ctime())
if __name__ == '__main__':
    main()
