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
    print "get cursor for " + db
    con = sqlite3.connect(db)
    con.isolation_level = None
    
    cur = con.cursor()

    return cur 

def get_nouns(cur, nouns_only=None):
    cond = ""
    if nouns_only is not None:
        cond = ",".join(map(str, nouns_only))
        cond = "where noun_md5 in (%s)" % cond

    res = cur.execute("""
        select noun_md5, noun from nouns
        %s
    """ % (cond)).fetchall()
    
    nouns = {}
    for r in res:
        nouns[r[0]] = r[1]
   
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

def fill_post_reply(cur):
    print "[%s]  fill chains_nouns" % (time.ctime())

    cur.execute("""
        insert or ignore into chains_nouns 
        select tc.post_id, n1.noun_md5, tc.reply_id, n2.noun_md5 
        from tweet_chains tc 
        inner join tweets_nouns n1 on n1.id = tc.post_id 
        inner join tweets_nouns n2 on n2.id = tc.reply_id
    """)

    fill_post_cnt(cur)

def fill_post_cnt(cur):
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

def get_some_noun_profiles(cur, posts_list, post_min_freq=10, profiles_table = "post_reply_cnt"):
    stats = cur.execute("""
        select p.post_md5, p.reply_md5, p.reply_cnt, p2.post_cnt
        from %(profiles_table)s p
        inner join post_cnt p2
        on p.post_md5 = p2.post_md5
        where
        p.post_md5 in (%(posts_list)s) 
        and p2.post_cnt > %(post_min_freq)d
        order by p2.post_cnt desc
    """ % {"profiles_table": profiles_table, "post_min_freq": post_min_freq, 
        "posts_list": ",".join(posts_list)}).fetchall()

    profiles_dict = {}

    for s in stats:
        post, reply, cnt, post_cnt  = s 
        if post not in profiles_dict:
            profiles_dict[post] = NounProfile(post, post_cnt=post_cnt) 
        profiles_dict[post].replys[reply] = cnt

    return profiles_dict

def get_noun_profiles(cur, post_min_freq, blocked_nouns, profiles_table = "post_reply_cnt"):
    limited = """
    p.post_md5 in (
            select post_md5 
            from post_cnt
            order by post_cnt desc
            limit 1000 
        )
    """
    create_given_tables(cur, ["valid_replys"])
    cur.execute("""
        insert or ignore into valid_replys
        select reply_md5 
        from (
            select count(*) as c, reply_md5
            from post_reply_cnt 
            group by reply_md5
            order by c desc
            limit 10000
        ) 
    """)

    stats = cur.execute("""
        select p.post_md5, p.reply_md5, p.reply_cnt, p2.post_cnt
        from %(profiles_table)s p
        inner join post_cnt p2
        on p.post_md5 = p2.post_md5
        inner join valid_replys v
        on p.reply_md5 = v.reply_md5
        where
        p2.post_cnt > %(post_min_freq)d
        and p.post_md5 not in (%(blocked_nouns)s) 
        and p.reply_md5 not in (%(blocked_nouns)s)
        order by p2.post_cnt desc
    """ % {"profiles_table": profiles_table, "post_min_freq": post_min_freq, 
        "blocked_nouns": blocked_nouns}).fetchall()

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

def get_title_context(title):
    cmd = "echo \""+  title + "\" | ./tomita-linux64 tomita/config.proto 2> /dev/null"
    logging.info(cmd)
    s = check_output(cmd, shell=True)
    logging.info(s)
    tree = ET.fromstring(s)
    nouns = tree.findall(".//Noun")  
    nouns = map(lambda x: x.get("val").lower(), nouns)
    logging.info(json.dumps(nouns, ensure_ascii=False))

    context = {}
    for n in nouns:
        noun_md5 = util.digest(n)
        if noun_md5 not in context:
            context[noun_md5] = 0
        context[noun_md5] += 1
    
    logging.info(context) 
    
    return context

def get_matching_contexts(cur, context):
    context_ids = map(str, context.keys())
    logging.info("Fetch possible matching contexts") 
    res = cur.execute("""
        select post_md5 
        from post_context_cnt
        where 
        reply_md5 in (%s)
        group by post_md5
    """ % (",".join(context_ids))).fetchall()

    possible_contexts = map(lambda x: str(x[0]), res)
    logging.info("Possible matching contexts: %s" % len(possible_contexts)) 

    logging.info("Fetch matching contexts") 
    ps = get_some_noun_profiles(cur, possible_contexts, profiles_table="post_context_cnt")

    query_profile = NounProfile(util.digest(str(context)))
    query_profile.replys = context
    
    logging.info("Compare contexts") 

    context_match = []
    for p in ps:
        context_match.append((p, query_profile.compare_with(ps[p]).sim))

    context_match = sorted(context_match, key=lambda x: x[1])[:10]
    context_match = map(lambda x: [x[0], 1 - x[1]], context_match)

    return context_match

def get_context_replys(cur, context_match):
    logging.info("Get similar nouns by reply") 

    replys = map(lambda x:  str(x[0]), context_match)
    res = cur.execute("""
        select post1_md5, post2_md5, sim
        from noun_similarity
        where 
        post1_md5 in (%(context_ids)s) or post2_md5 in (%(context_ids)s)
        order by sim
        limit 100
    """ % {"context_ids": ",".join(replys)}).fetchall() 

    repl_sims = {} 

    for r in res:
        p1, p2, s = r
        if p1 not in repl_sims:
            repl_sims[p1] = {}
        if p2 not in repl_sims:
            repl_sims[p2] = {}
        repl_sims[p1][p2] = s
        repl_sims[p2][p1] = s
      
    context_replys = []
    for context in context_match:
        context_id, context_sim = context
        if context_id not in repl_sims:
            continue
        for k in repl_sims[context_id]:
            if k not in repl_sims:
                continue
            k_sim = repl_sims[context_id][k]
            context_replys.append([k, context_sim * (1 - k_sim), (1 - k_sim) , 
            {"context_md5": context_id,
            "sim": context_sim}])

    return context_replys

def get_merged_contexts(context_match, context_replys):
    merge_context = sorted(context_replys + context_match, key=lambda x: x[1], reverse = True)

    merge_context_keys = []

    merge_context2 = []
    for m in merge_context:
        if m[1] in merge_context_keys:
            continue
        merge_context2.append(m)
    merge_context2 = merge_context2[:20]

    return merge_context2
 
def get_sim_nouns_by_context(cur, title):
    logging.info("get_title_context")
    context = get_title_context(title)
    if len(context) == 0:
        return []

    logging.info("get_matching_contexts")
    context_match = get_matching_contexts(cur, context)
    logging.info("got %s matching contexts" % len(context_match))
    
    logging.info("get_context_replys")
    context_replys = get_context_replys(cur, context_match)
    logging.info("got %s matching context replys" % len(context_replys))

    logging.info("get_merged_contexts")
    merge_context = get_merged_contexts(context_match, context_replys)

    return map(lambda x: {"noun_md5": x[0], "sim": x[1]}, merge_context)

def save_title_context(cur, title, context):
    title_md5 = util.digest(title)
    cur.execute("""
        insert or ignore into titles
        values (?, ?)
    """, (title, title_md5))

    cur.executemany("""
        insert or ignore into titles_profiles
        values (?, ?, ?)
    """, map(lambda x: (title_md5, x["noun_md5"], x["sim"]), context))

def get_similar_titles(cur, title, context, top=10):
    title_md5 = util.digest(title)
    res = cur.execute("""
        select title_md5, noun_md5, sim
        from titles_profiles
        where title_md5 != ?
    """, (title_md5,)).fetchall()

    ps = {}
    for r in res:
        t_md5, n_md5, sim = r
        if t_md5 not in ps:
            ps[t_md5] = NounProfile(t_md5)
        ps[t_md5].replys[n_md5] = sim

    context_profile = NounProfile(1)
    for c in context:
        context_profile.replys[c["noun_md5"]] = c["sim"]
    
    sim_profs = {} 
    for p in ps.keys():
        sim_profs[p] = ps[p].compare_with(context_profile).sim

    sim_profs_top = sorted(sim_profs.keys(), key=lambda x: sim_profs[x])[:top]

    res = cur.execute("""
        select title_md5, title from titles
        where title_md5 in (%s)
    """ % ",".join(map(str,sim_profs_top))).fetchall()

    sim_titles = {}
    for t in res:
        t_md5, title = t
        sim_titles[t_md5] = title
    logging.warn(sim_titles)

    sim_docs = map(lambda x: {"title": sim_titles[x], "title_md5": x, "sim": sim_profs[x]}, sim_profs_top)
            
    return sim_docs


               
