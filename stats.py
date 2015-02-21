#!/usr/bin/python
# -*- coding: utf-8 -*-
import sqlite3
import time
import math
import logging, logging.config
import xml.etree.ElementTree as ET
from subprocess import check_output
import json

from profile import NounProfile, ProfileCompare
import util

def get_cursor(db):
    logging.info("get cursor for " + db)
    con = sqlite3.connect(db)
    con.isolation_level = None
    
    cur = con.cursor()

    return cur 

def get_main_cursor(db_dir):
    return get_cursor(db_dir + "/tweets.db")

def get_sources(cur, nouns_only=None):
    return get_nounlikes(cur, nouns_only, "sources")

def get_nouns(cur, nouns_only=None):
    return get_nounlikes(cur, nouns_only, "nouns")

def get_nounlikes(cur, nouns_only, nouns_table):
    logging.info("start")
    cond = ""
    if nouns_only is not None:
        cond = ",".join(map(str, nouns_only))
        cond = "where noun_md5 in (%s)" % cond

    res = cur.execute("""
        select noun_md5, noun 
        from %s
        %s
    """ % (nouns_table, cond)).fetchall()
    
    nouns = {}
    for r in res:
        nouns[r[0]] = r[1]
   
    logging.info("done")

    return nouns

def get_noun_trend(cur):
    cur.execute("""
        select noun_md5, trend
        from noun_trend
    """)

    noun_trend = {}
    for r in cur.fetchall():
        noun_md5, trend = r
        noun_trend[noun_md5] = trend

    return noun_trend
        

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


CREATE_TABLES = {
    "tweets": """
        CREATE TABLE IF NOT EXISTS tweets
        (
            id integer,
            tw_text text,
            username text,
            in_reply_to_username text,
            in_reply_to_id integer,
            created_at integer,
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
    "chains_nouns_all": """
        CREATE TABLE IF NOT EXISTS chains_nouns_all(
            p_id integer,
            p_md5 integer,
            r_id integer,
            r_md5 integer,
            created_at integer,
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
    "statuses_progress": """
        CREATE TABLE IF NOT EXISTS statuses_progress (
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
    "tweets_words": """
        CREATE TABLE IF NOT EXISTS tweets_words(
            id integer,
            noun_md5 integer,
            source_md5 integer,
            PRIMARY KEY(id, noun_md5, source_md5)
        )
    """,
    "word_time_cnt": """
        CREATE TABLE IF NOT EXISTS word_time_cnt(
            word_md5 integer,
            tenminute integer,
            cnt integer,
            PRIMARY KEY(word_md5, tenminute)
        )
    """,
    "tweets_words_simple": """
        CREATE TABLE IF NOT EXISTS tweets_words_simple(
            noun_md5 integer,
            source_md5 integer
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
    """,
    "relevant": """
        CREATE TABLE IF NOT EXISTS relevant (
            cluster_date text,
            relevant text,
            PRIMARY KEY (cluster_date)
        )
    """,
    "titles": """
        CREATE TABLE IF NOT EXISTS titles (
            title text,
            title_md5 integer,
            PRIMARY KEY (title_md5)
        )
    """,
    "titles_profiles": """
        CREATE TABLE IF NOT EXISTS titles_profiles (
            title_md5 integer,
            noun_md5 integer,
            sim float,
            PRIMARY KEY (title_md5, noun_md5)
        )
    """,
    "valid_replys" : """
        CREATE TABLE IF NOT EXISTS valid_replys (
            reply_md5 integer,
            PRIMARY KEY (reply_md5)
        )
    """, 
    "noun_trend": """
        CREATE TABLE IF NOT EXISTS noun_trend (
            noun_md5 integer,
            trend float,
            PRIMARY KEY (noun_md5)
        )
    """,
    "lemma_word_pairs": """
        CREATE TABLE IF NOT EXISTS lemma_word_pairs (
            noun1_md5 integer,
            noun2_md5 integer,
            source1_md5 integer,
            source2_md5 integer,
            cnt integer default 0
        )
    """ 
}

def cr(cur):
    create_given_tables(cur, ["tweets", "users"]) 

def create_given_tables(cur, tables):
    if isinstance(tables, list):
        for t in tables:
            cur.execute(CREATE_TABLES[t])
    if isinstance(tables, dict):
        for name in tables:
            like = tables[name]
            logging.info("create table %s like %s" %(name, like))
            cur.execute(CREATE_TABLES[like].replace(like, name, 1))

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

def _log_execute(cur, query):
    logging.info(query)
    return cur.execute(query) 

def get_noun_profiles(cur, post_min_freq, blocked_nouns, profiles_table = "post_reply_cnt"):
    logging.info("start") 
    cur.execute("attach ':memory:' as tmp") 
    create_given_tables(cur, {"tmp.valid_replys": "valid_replys"})
    cur.execute("""
        insert or ignore into tmp.valid_replys
        select reply_md5 
        from (
            select count(*) as c, reply_md5
            from post_reply_cnt 
            where reply_md5 not in (%s)
            group by reply_md5
            order by c desc
            limit 8000
        )
    """ % blocked_nouns)

    cmd = """
        select p.post_md5, p.reply_md5, p.reply_cnt, p2.post_cnt
        from %(profiles_table)s p
        inner join post_cnt p2
        on p.post_md5 = p2.post_md5
        inner join tmp.valid_replys v
        on p.reply_md5 = v.reply_md5
        where
        p2.post_cnt > %(post_min_freq)d
        and p.post_md5 not in (%(blocked_nouns)s) 
        and p.reply_md5 not in (%(blocked_nouns)s)
        order by p2.post_cnt desc
    """ % {"profiles_table": profiles_table, "post_min_freq": post_min_freq, 
        "blocked_nouns": blocked_nouns}

    #logging.debug(cmd)

    cur.execute(cmd)

    profiles_dict = {}

    while True:
        s = cur.fetchone()
        if s is None:
            break
        post, reply, cnt, post_cnt  = s 
        if post not in profiles_dict:
            profiles_dict[post] = NounProfile(post, post_cnt=post_cnt) 
        profiles_dict[post].replys[reply] = cnt

    logging.info("done")

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

    #if entropy == 0:
    #    logging.warn("Post %s. Couldnt find entropy (possibly all replys are equally probable" % profile.post)
    #    return 1

    entropy *= -1

    for r in profile.replys:
        profile.replys[r] = profile.replys[r] / entropy

    return entropy

@util.time_logger
def tfidf(profiles_dict, total_docs):
    inverse_reply_freq = {}
    for k in profiles_dict.keys():
        profile = profiles_dict[k]
        for r in profile.replys:
            if r not in inverse_reply_freq:
                inverse_reply_freq[r] = 0
            inverse_reply_freq[r] += profile.replys[r]

    for r in inverse_reply_freq:
        inverse_reply_freq[r] = math.log(float(total_docs) / inverse_reply_freq[r] )

    for k in profiles_dict.keys():
        profile = profiles_dict[k]
        total_doc_words = 0
        for r in profile.replys:
            total_doc_words += profile.replys[r]
        for r in profile.replys:
            profile.replys[r] = (float(profile.replys[r]) / total_doc_words) * inverse_reply_freq[r]
        
@util.time_logger
def weight_profiles_with_entropy(cur, profiles_dict, nouns):
    logging.info("start")
    post_cnt = get_noun_cnt(cur) 
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

    logging.info("done")

def noun_profiles_tfidf(cur, post_min_freq, blocked_nouns, nouns_limit, total_docs):
    profiles_dict = get_noun_profiles(cur, post_min_freq, blocked_nouns, "post_reply_cnt")

    logging.info("Profiles len: %s" % len(profiles_dict))
    if False or len(profiles_dict) > nouns_limit:
        short_profiles_dict = {}
        
        for k in sorted(profiles_dict.keys(), key=lambda x: profiles_dict[x].post_cnt, reverse=True)[:nouns_limit]:
            short_profiles_dict[k] = profiles_dict[k]

        profiles_dict = short_profiles_dict

        logging.info("Short-list profiles len: %s" % len(profiles_dict))

    tfidf(profiles_dict, total_docs) 
   
    return profiles_dict


def setup_noun_profiles(cur, tweets_nouns, nouns, post_min_freq, blocked_nouns, nouns_limit, profiles_table="post_reply_cnt"):
    profiles_dict = get_noun_profiles(cur, post_min_freq, blocked_nouns, profiles_table)

    #set_noun_profiles_tweet_ids(profiles_dict, tweets_nouns)
    logging.info("Profiles len: %s" % len(profiles_dict))
    if False or len(profiles_dict) > nouns_limit:
        short_profiles_dict = {}
        
        for k in sorted(profiles_dict.keys(), key=lambda x: profiles_dict[x].post_cnt, reverse=True)[:nouns_limit]:
            short_profiles_dict[k] = profiles_dict[k]

        profiles_dict = short_profiles_dict

        logging.info("Short-list profiles len: %s" % len(profiles_dict))

    set_noun_profiles_total(cur, profiles_dict, post_min_freq, blocked_nouns)

    weight_profiles_with_entropy(cur, profiles_dict, nouns) 
   
    return profiles_dict

              
