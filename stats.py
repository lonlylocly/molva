#!/usr/bin/python
# -*- coding: utf-8 -*-
import sqlite3
import time

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

def get_post_reply_tweets(cur, post_noun, reply_noun, tweets_nouns):

    #print "[%s] get_post_reply_tweets %s, %s " % (time.ctime(), post_noun, reply_noun)
    post_ids = tweets_nouns[int(post_noun)]
    reply_ids = tweets_nouns[int(reply_noun)]
    res = cur.execute("""
        select id, username
        from tweets
        where in_reply_to_id in ( %s )
        and id in ( %s )
    """ % (",".join(post_ids), ",".join(reply_ids))).fetchall()

    #print "[%s] get_post_reply_tweets %s, %s (done)" % (time.ctime(), post_noun, reply_noun)
    return res

    #"""
    #    select pr.post_md5, count(tw.id) as replys_cnt 
    #    from post_reply_cnt pr
    #    inner join tweets_nouns tn
    #    on pr.post_md5 = tn.noun_md5
    #    inner join tweets tw
    #    on tw.in_reply_to_id = tn.id
    #"""

def get_post_replys_tweets(cur, tweets_nouns, post_noun):

    #print "[%s] get_post_reply_tweets %s, %s " % (time.ctime(), post_noun, reply_noun)
    post_ids = tweets_nouns[int(post_noun)]
    res = cur.execute("""
        select distinct id
        from tweets
        where in_reply_to_id in ( %s )
    """ % ",".join(post_ids)).fetchall()

    #print "[%s] get_post_reply_tweets %s, %s (done)" % (time.ctime(), post_noun, reply_noun)
    return res


def get_post_tweets(cur, post_noun):
    res = cur.execute("""
        select t.id, t.username
        from tweets t
        inner join tweets t2
        on t.id = t2.in_reply_to_id
        where t.id in (
            select id from tweets_nouns
            where noun_md5 = %d
        )
        group by t.id, t.username
    """ % (post_noun)).fetchall()

    return res


