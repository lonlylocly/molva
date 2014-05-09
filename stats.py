#!/usr/bin/python
# -*- coding: utf-8 -*-
import sqlite3
import time

def get_cursor(db):
    print "get cursor for " + db
    con = sqlite3.connect(db)
    con.isolation_level = None
    
    cur = con.cursor()

    return cur 

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

    post_ids = tweets_nouns[int(post_noun)]
    reply_ids = tweets_nouns[int(reply_noun)]
    res = cur.execute("""
        select id, username
        from tweets
        where in_reply_to_id in ( %s )
        and id in ( %s )
    """ % (",".join(post_ids), ",".join(reply_ids))).fetchall()

    return res


def get_post_replys_tweets(cur, tweets_nouns, post_noun):

    post_ids = tweets_nouns[int(post_noun)]
    res = cur.execute("""
        select distinct id
        from tweets
        where in_reply_to_id in ( %s )
    """ % ",".join(post_ids)).fetchall()

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

def get_post_tweets_cnt(cur):
    res = cur.execute("""
        select noun_md5, count(*)
        from tweets_nouns tn
        inner join tweet_chains tc
        on tn.id = tc.post_id
        group by noun_md5
    """)

    post_tweets_cnt = {}
    for post_cnt in res:
        post, cnt = post_cnt
        post_tweets_cnt[post] = cnt

    return post_tweets_cnt

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

CREATE_TABLES = {
    "tweets": """
        CREATE TABLE IF NOT EXISTS tweets
        (
            id integer,
            tw_text text,
            username text,
            in_reply_to_username text,
            in_reply_to_id integer,
            created_at text,
            PRIMARY KEY (id)
        )
    """,
    "users": """
        CREATE TABLE IF NOT EXISTS users
        (
            username text,
            since_id integer default 0,
            reply_cnt integer default 0,
            blocked_user default 0,
            done_time text default '',
            PRIMARY KEY (username)
        )
    """,
    "post_reply_cnt": """
        CREATE TABLE IF NOT EXISTS post_reply_cnt (
            post_md5 integer,
            reply_md5 integer, 
            reply_cnt integer,
            PRIMARY KEY(post_md5, reply_md5)
        )
    """,
    "post_cnt": """
        CREATE TABLE IF NOT EXISTS post_cnt ( 
            post_md5 integer, 
            post_cnt integer, 
            primary key(post_md5)
        )
    """,

    "tweet_chains": """
        CREATE TABLE IF NOT EXISTS tweet_chains (
            post_id integer,
            reply_id integer,
            PRIMARY KEY (post_id, reply_id)
        )
    """,
    "chains_nouns": """
        CREATE TABLE IF NOT EXISTS chains_nouns(
            p_id integer,
            p_md5 integer,
            r_id integer,
            r_md5 integer,
            PRIMARY KEY (p_id, p_md5, r_id, r_md5)
        )
    """
}

def cr(cur):
    create_given_tables(cur, ["tweets", "users"]) 

def create_given_tables(cur, tables):
    for t in tables:
        cur.execute(CREATE_TABLES[t])

def create_tables(cur):
    create_given_tables(cur, ["post_reply_cnt", "post_cnt", "tweet_chains", "chains_nouns", "tweets", "users"]) 

def fill_tweet_chains(cur, date):
    print "[%s]  fill tweet_chains" % (time.ctime())

    cur.execute("""
        insert into tweet_chains
        select t1.id, t2.id 
        from tweets t1
        inner join molva.tweets t2
        on t1.id = t2.in_reply_to_id
        where t2.date LIKE "%s%%"
    """ % (date))

    print "[%s] done fill tweet_chains" % (time.ctime())

def fill_post_reply(cur):
    print "[%s]  fill chains_nouns" % (time.ctime())

    cur.execute("""
        insert or ignore into chains_nouns 
        select tc.post_id, n1.noun_md5, tc.reply_id, n2.noun_md5 
        from tweet_chains tc 
        inner join tweets_nouns n1 on n1.id = tc.post_id 
        inner join tweets_nouns n2 on n2.id = tc.reply_id
    """)

    print "[%s] fill post_reply_cnt" % (time.ctime())

    cur.execute("""
        insert or ignore into post_reply_cnt (post_md5, reply_md5, reply_cnt) 
        select p_md5, r_md5, count(*) 
        from chains_nouns 
        group by p_md5, r_md5;
    """)

    print "[%s] fill post_cnt" % (time.ctime())

    cur.execute("""
        insert or ignore into post_cnt 
        select p_md5,  count(*) 
        from (
            select p_id, p_md5 
            from chains_nouns 
            group by p_md5, p_id
        ) group by p_md5
    """)

    print "[%s] done filling" % (time.ctime())
