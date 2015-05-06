#!/usr/bin/python
# -*- coding: utf-8 -*-
import sqlite3
import time
import math
import logging, logging.config
import xml.etree.ElementTree as ET
import json
from datetime import datetime, timedelta, date
import MySQLdb
import MySQLdb.cursors

from molva.profile import NounProfile, ProfileCompare
import molva.util as util

def get_cursor(db):
    logging.info("get cursor for " + db)
    con = sqlite3.connect(db)
    con.isolation_level = None
    
    cur = con.cursor()

    return cur 

def get_mysql_cursor(settings,streaming=False):
    
    db = MySQLdb.connect(host="localhost", user=settings["mysql_user"], passwd=settings["mysql_password"], db="molva",
        cursorclass = MySQLdb.cursors.SSCursor)

    cur = db.cursor()

    return cur

def get_main_cursor(db_dir):
    return get_cursor(db_dir + "/tweets.db")

@util.time_logger
def get_sources(cur, nouns_only=None):
    return get_nounlikes(cur, nouns_only, "sources")

@util.time_logger
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
            PRIMARY KEY(tenminute, word_md5)
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
    """,
    "quality_marks": """
        CREATE TABLE IF NOT EXISTS quality_marks (
            update_time integer,
            username text,
            exp_name text,
            exp_descr text,
            marks text
        )
    """,
    "word_mates": """
        CREATE TABLE IF NOT EXISTS word_mates (
            word1 integer,
            word2 integer,
            tenminute integer,
            cnt integer,            
            PRIMARY KEY(tenminute, word1, word2)
        )   
    """,
    "word_mates_sum": """
        CREATE TABLE IF NOT EXISTS word_mates_sum (
            word1 integer,
            word2 integer, 
            cnt integer
        )
    """

}

MYSQL_CREATE_TABLES = {
    "word_time_cnt": """
        CREATE TABLE IF NOT EXISTS word_time_cnt (
            word_md5 int unsigned not null,
            tenminute bigint unsigned not null,
            cnt int unsigned not null,
            PRIMARY KEY(tenminute, word_md5)   
        ) ENGINE=MYISAM DEFAULT CHARSET=utf8
    """,
    "word_mates": """
        CREATE TABLE IF NOT EXISTS word_mates (
            word1 int unsigned not null,
            word2 int unsigned not null,
            tenminute bigint unsigned not null,
            cnt int unsigned not null,            
            PRIMARY KEY(tenminute, word1, word2)
        )  ENGINE=MYISAM DEFAULT CHARSET=utf8  
    """,
    "word_mates_sum": """
        CREATE TABLE IF NOT EXISTS word_mates_sum (
            word1 int unsigned not null,
            word2 int unsigned not null,
            cnt int unsigned not null,            
            PRIMARY KEY(word1, word2)
        )  ENGINE=MYISAM DEFAULT CHARSET=utf8  
    """,
    "bigram_day": """
        CREATE TABLE IF NOT EXISTS bigram_day (
            word1 int unsigned not null,
            word2 int unsigned not null,
            source1 int unsigned not null,
            source2 int unsigned not null,
            tenminute bigint unsigned not null,
            cnt int unsigned not null,
            PRIMARY KEY(tenminute, word1, word2, source1, source2)
        ) ENGINE=MYISAM DEFAULT CHARSET=utf8  
    """

}

def cr(cur):
    create_given_tables(cur, ["tweets", "users"]) 

def create_mysql_tables(cur, tables):
    _create_given_tables(cur, tables, MYSQL_CREATE_TABLES)

def create_given_tables(cur, tables):
    _create_given_tables(cur, tables, CREATE_TABLES)

def _create_given_tables(cur, tables, templates):
    if isinstance(tables, list):
        for t in tables:
            cur.execute(templates[t])
    if isinstance(tables, dict):
        for name in tables:
            like = tables[name]
            logging.info("create table %s like %s" %(name, like))
            query = templates[like].replace(like, name, 1)
            #logging.debug(query)
            cur.execute(query)

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
    (valid_replys_cnt) = cur.execute("select count(*) from tmp.valid_replys").fetchone()
    logging.info("valid_replys cnt: %s" % valid_replys_cnt)
    logging.info("post_min_freq: %s" % post_min_freq)

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
    rows_seen = 0
    profiles_dict = {}
    while True:
        s = cur.fetchone()
        if s is None:
            break
        rows_seen += 1
        post, reply, cnt, post_cnt  = s 
        if post not in profiles_dict:
            profiles_dict[post] = NounProfile(post, post_cnt=post_cnt) 
        profiles_dict[post].replys[reply] = cnt
    
    logging.info("rows seen: %s" % rows_seen)

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
    #post_cnt = get_noun_cnt(cur) 
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

def _add_total_to_profiles(profiles_dict, trash_words):
    trash_words_md5 = map(util.digest, trash_words)
    total_md5 = util.digest('__total__') 
    total = NounProfile(total_md5, post_cnt=0)
    for p in profiles_dict:
        if p not in trash_words_md5:
            continue
        profile = profiles_dict[p]
        for reply in profile.replys:
            if reply not in total.replys:
                total.replys[reply] = 0
            total.replys[reply] += profile.replys[reply]
        total.post_cnt += profile.post_cnt
    profiles_dict[total_md5] = total

def _filter_swear_words(profiles_dict, swear_words):
    swear_words_md5 = map(util.digest, swear_words)
    filtered = {}
    filtered_cnt = 0
    for p in profiles_dict:
        if p not in swear_words_md5:
            filtered[p] = profiles_dict[p]
        else:
            filtered_cnt +=1
    logging.info("Filtered swear words: %s" % filtered_cnt)

    return filtered

def setup_noun_profiles(cur, tweets_nouns, nouns, post_min_freq, blocked_nouns, nouns_limit, profiles_table="post_reply_cnt", trash_words=None,
    swear_words=None):
    profiles_dict = get_noun_profiles(cur, post_min_freq, blocked_nouns, profiles_table)

    #set_noun_profiles_tweet_ids(profiles_dict, tweets_nouns)
    logging.info("Profiles len: %s" % len(profiles_dict))
    if swear_words is not None:
        profiles_dict = _filter_swear_words(profiles_dict, swear_words)

    if False or len(profiles_dict) > nouns_limit:
        short_profiles_dict = {}
       
        for k in sorted(profiles_dict.keys(), key=lambda x: profiles_dict[x].post_cnt, reverse=True)[:nouns_limit]:
            short_profiles_dict[k] = profiles_dict[k]

        profiles_dict = short_profiles_dict

        logging.info("Short-list profiles len: %s" % len(profiles_dict))

    set_noun_profiles_total(cur, profiles_dict, post_min_freq, blocked_nouns)

    weight_profiles_with_entropy(cur, profiles_dict, nouns) 

    if trash_words is not None:
        _add_total_to_profiles(profiles_dict, trash_words)
   
    return profiles_dict

@util.time_logger
def get_word_cnt(db_dir, utc_now=datetime.utcnow()):
    day_ago = (utc_now - timedelta(1)).strftime("%Y%m%d%H%M%S")
    day_ago_tenminute = day_ago[:11]
    logging.info("Time left bound: %s" % day_ago_tenminute)
    settings = json.load(open('global-settings.json', 'r'))
    word_cnt = {}
    for day in [1, 0]:
        date = (utc_now - timedelta(day)).strftime("%Y%m%d")
        word_time_cnt_table = "word_time_cnt_%s" % date
        mcur = get_mysql_cursor(settings)
        create_mysql_tables(mcur, {word_time_cnt_table: "word_time_cnt"})
        mcur.execute("""
                select word_md5, sum(cnt) 
                from %s 
                where tenminute > %s
                group by word_md5
        """ % (word_time_cnt_table, day_ago_tenminute))

        row_cnt = 0    
        while True:
            res = mcur.fetchone()
            if res is None:
                break
            word_md5, cnt = res
            if word_md5 not in word_cnt:
                word_cnt[word_md5] = 0 
            word_cnt[word_md5] += cnt
            
            row_cnt += 1
            if row_cnt % 100000 == 0:
                logging.info('Seen %s rows' % row_cnt)

    return word_cnt             
