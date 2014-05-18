#!/usr/bin/python
# -*- coding: utf-8 -*-
import sqlite3
import time
import math
import logging, logging.config

from profile import NounProfile, ProfileCompare

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
    """,
    "tomita_progress": """
        CREATE TABLE IF NOT EXISTS tomita_progress (
            id integer,
            id_done integer default 0,
            PRIMARY KEY (id, id_done)
        )
    """,
    "nouns": """
        CREATE TABLE IF NOT EXISTS nouns (
            noun_md5 integer,
            noun text,
            PRIMARY KEY(noun_md5)
        )
    """,
    "tweets_nouns": """
        CREATE TABLE IF NOT EXISTS tweets_nouns(
            id integer,
            noun_md5 integer,
            PRIMARY KEY(id, noun_md5)
        )
    """,
    "noun_similarity": """
        CREATE TABLE IF NOT EXISTS noun_similarity (
            post1_md5 integer,
            post2_md5 integer,
            sim float,
            PRIMARY KEY (post1_md5, post2_md5)
        ) 
    """,
    "table_stats": """
        CREATE TABLE IF NOT EXISTS table_stats (
            table_name text not null,
            table_date text not null,
            row_count integer not null,
            update_time text,
            PRIMARY KEY (table_name, table_date)
        )
    """,
    "clusters": """
        CREATE TABLE IF NOT EXISTS clusters (
            cluster_date text,
            k integer,
            cluster text,
            PRIMARY KEY (cluster_date, k)
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

def fill_tweet_chains(cur):
    print "[%s]  fill tweet_chains" % (time.ctime())

    cur.execute("""
        insert or ignore into tweet_chains
        select t1.id, t2.id 
        from tweets t1
        inner join tweets t2
        on t1.id = t2.in_reply_to_id
    """)

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

def _log_execute(cur, query):
    logging.info(query)
    return cur.execute(query) 

def get_noun_profiles(cur, post_min_freq, blocked_nouns):
    stats = _log_execute(cur, """
        select p.post_md5, p.reply_md5, p.reply_cnt, p2.post_cnt
        from post_reply_cnt p
        inner join post_cnt p2
        on p.post_md5 = p2.post_md5
        where p2.post_cnt > %d
        and p.post_md5 not in (%s) 
        and p.reply_md5 not in (%s)
        order by p2.post_cnt desc
    """ % (post_min_freq, blocked_nouns, blocked_nouns)).fetchall()

    profiles_dict = {}

    for s in stats:
        post, reply, cnt, post_cnt  = s 
        if post not in profiles_dict:
            profiles_dict[post] = NounProfile(post, post_cnt=post_cnt) 
        profiles_dict[post].replys[reply] = cnt

    return profiles_dict

def set_noun_profiles_tweet_ids(profiles_dict, tweet_nouns):
    for x in profiles_dict.keys():
        profiles_dict[x].post_tweet_ids = tweet_nouns[x] 

def set_noun_profiles_total(cur, profiles_dict, post_min_freq, blocked_nouns):
    total_stats = cur.execute("""
        select p.post_md5, sum(p.reply_cnt)
        from post_reply_cnt p
        inner join post_cnt p2
        on p.post_md5 = p2.post_md5
        where p2.post_cnt > %d
        and p.post_md5 not in (%s)
        and reply_md5 not in (%s)
        group by p.post_md5
        order by p2.post_cnt desc
    """ % (post_min_freq, blocked_nouns, blocked_nouns)).fetchall()

    for s in total_stats:
        post = int(s[0])
        reply_cnt = int(s[1])

        #assert post in profiles_dict
        if post not in profiles_dict:
            continue

        profiles_dict[post].total = reply_cnt 

def count_freq(d, freq):
    if freq in d:
        d[freq] += 1
    else:
        d[freq] = 1

def count_entropy(profile, repl_p, tot_profiles):
    entropy = 0
    for r in profile.replys:
        freq = profile.replys[r]
        p = (repl_p[r][freq] + 0.0) / tot_profiles
        entropy += p * math.log(p)

    entropy *= -1

    for r in profile.replys:
        profile.replys[r] = profile.replys[r] / entropy

    return entropy
 
def weight_profiles_with_entropy(profiles_dict, nouns):
    replys = [] 
    for p in profiles_dict:
        profiles_dict[p].apply_log()
        replys += profiles_dict[p].replys.keys()

    replys = list(set(replys))
    repl_ps = {}
    # посчитаем сколько раз заданная частота ответа встречается в профилях
    for r in replys:
        repl_p = {}
        repl_ps[r] = repl_p 
        for pr in profiles_dict:
            if r in profiles_dict[pr].replys:
                count_freq(repl_p, profiles_dict[pr].replys[r])
            else:
                count_freq(repl_p, 0)

    # на основе этих частот посчитаем энтропию, и нормируем ею все profile.replys 
    for pr in profiles_dict:
        count_entropy(profiles_dict[pr], repl_ps, len(profiles_dict.keys()))


