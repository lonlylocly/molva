#!/usr/bin/python
# -*- coding: utf-8 -*-
import sqlite3
import sys,codecs
import time
import math
import json
import re

from stats import get_tweets_nouns, get_post_replys_tweets, get_nouns, get_post_tweets_cnt 
from util import digest

sys.stdout = codecs.getwriter('utf8')(sys.stdout)

db = 'more_replys2.db'

POST_MIN_FREQ = 30
REPLY_MIN_FREQ = 1 

REPLY_REL_MIN = 0.005

BLOCKED_NOUNS_LIST = u"""
уж
че
ага
нет
чё
эта
чё-то
оха
до
"""
BLOCKED_NOUNS_LIST += u"\n".join(list(u"абвгдеёжзиклмнопрстуфхцчшщыьъэюя"))

BLOCKED_NOUNS = ",".join(map( lambda x: str(digest(x)), BLOCKED_NOUNS_LIST.split("\n")))


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
        and post_md5 not in (%s) and reply_md5 not in (%s)
    """ % (POST_MIN_FREQ, BLOCKED_NOUNS, BLOCKED_NOUNS)).fetchall()

    stats_dict = {}

    for s in stats:
        post = int(s[0])
        reply = int(s[1])
        cnt = int(s[2])
        if post not in stats_dict:
            stats_dict[post] = {reply: cnt}
        stats_dict[post][reply] = cnt

    return stats_dict

def get_noun_profiles_total(cur):
    stats_dict = {}
    total_stats = cur.execute("""
        select post_md5, sum(reply_cnt)
        from post_reply_cnt
        where post_cnt > %d
        and reply_md5 not in (%s)
        group by post_md5
    """ % (POST_MIN_FREQ, BLOCKED_NOUNS)).fetchall()

    for s in total_stats:
        post = int(s[0])
        reply_cnt = int(s[1])
        if post not in stats_dict:
            stats_dict[post] = {}
        stats_dict[post] = reply_cnt 
    
    return stats_dict

def get_noun_profiles_noise(cur):
    stats_dict = {}

    noise_stats = cur.execute("""
        select post_md5, sum(reply_cnt)
        from post_reply_cnt
        where post_cnt > %d
        and reply_cnt <= %d
        group by post_md5
    """ % (POST_MIN_FREQ, REPLY_MIN_FREQ )).fetchall()

    for s in noise_stats:
        post = int(s[0])
        reply_cnt = int(s[1])
        if post not in stats_dict:
            stats_dict[post] = {}
        stats_dict[post] = reply_cnt

    return stats_dict
 
def check_stats_row(key, row):       
    ok_cnt = 0
    noise_cnt = 0
    for reply in row:
        if reply == 0:
            noise_cnt += row[reply]
        else:
            ok_cnt += row[reply]

    try: 
        assert (ok_cnt + noise_cnt) <= 1.001 
        assert (ok_cnt + noise_cnt) > 0.999
    except Exception as e:
        print "key: %s; ok_cnt: %s; noise_cnt: %s" % (key, ok_cnt, noise_cnt)
        raise e

def get_rel_stats(abs_stats, total_stats):
    rel_stats = {}
    for post in abs_stats:
        total = total_stats[post]
        rel_stats[post] = {}
        rel_stats[post][0] = 0.0
        for reply in abs_stats[post]:
            repl_portion = (abs_stats[post][reply] + 0.0)/ total
            if repl_portion <= REPLY_REL_MIN:
                rel_stats[post][0] += repl_portion
            else:
                rel_stats[post][reply] = repl_portion

    return rel_stats

def get_damping_coeff(profile):
    coeff = 1
    if 0 in profile and profile[0] != 1 :
        coeff = 1 / (1 - profile[0])

    return coeff
         

def compare_profiles(prof1, prof2):
    total = 0

    #print json.dumps(prof1, indent=4)
    #print json.dumps(prof2, indent=4)

    #print "reply\tx1\tx2\tpart" 
    damp1 = get_damping_coeff(prof1) 
    damp2 = get_damping_coeff(prof2) 

    for reply in (set(prof1.keys()) | set(prof2.keys())):
        if reply == 0:
            continue
        x1 = damp1 * prof1[reply] if reply in prof1 else 0 
        x2 = damp2 * prof2[reply] if reply in prof2 else 0 
        part = math.fabs(x1 - x2)
        #print "%s\t%s\t%s\t%s" % (reply, x1, x2, part)
        total += part
    
    total = total /2
    
    #print "total: %s" % total

    return total

def get_sim_map(rel_stats):
    sim_map = {}

    posts = rel_stats.keys()

    cnt = 0
    long_cnt = 0
    max_cnt = len(posts) * len(posts) / 2

    f = open("noun-profiles.txt", "w")
    print "[%s] Todo:  %s" % (time.ctime(), max_cnt)
    for i in range(0, len(posts)):
        post1 = posts[i]
        sim_map[post1] = {}
        for j in range(i + 1, len(posts)):
            post2 = posts[j]
            cmp_prof = compare_profiles(rel_stats[post1], rel_stats[post2])
            #sim_map[post1][post2] 
            cnt += 1
            f.write("%s\t%s\t%s\n" % (post1, post2, cmp_prof))
        if long_cnt * 5e5 < cnt:
            print "[%s] done so far %s" % (time.ctime(), cnt)
            long_cnt += 1
        sim_map = {}
    f.close()

def debug_sim_net(rel_stats, nouns, post_tweets_cnt):
    posts = rel_stats.keys()

    cnt = 0
    long_cnt = 0
    max_cnt = len(posts) * len(posts) / 2

    top_sims = {}
    for i in range(0, len(posts)):
        post1 = posts[i]
        cmps = []
        for j in range(0, len(posts)):
            if i == j:
                continue
            post2 = posts[j]
            #print "compare %s and %s" % (post1, post2)
            cmp_prof = compare_profiles(rel_stats[post1], rel_stats[post2])
            cnt += 1
            cmps.append((post2, cmp_prof))
        cmps = map(lambda x: x, sorted(cmps, key=lambda x: x[1]))

        write_noun_sim_info(post1, cmps, nouns, rel_stats[post1], post_tweets_cnt[post1])
        top_sims[post1] = cmps[0] 

    return top_sims

def write_noun_sim_info(post, cmps, nouns, profile, post_tweet_cnt):
    filename = "./sim-net/" + str(post) + ".html"
    print "[%s] write file %s " % (time.ctime(), filename)
    fout = codecs.open(filename,'w',encoding='utf8')
    fout.write("<html><head><meta charset=\"UTF-8\"><title>" + nouns[post] + "</title></head><body>\n<table border=\"1\">")

    heading = """
<html><head><meta charset=\"UTF-8\"></head><body>
"""   
    fout.write(heading)

    fout.write("<h2>" + nouns[post] + "<h2>\n")
    fout.write(u"<p>Всего постов с этим словом: %d</p>" % post_tweet_cnt )
    fout.write(u"<h3>Топ-10 похожих постов</h3>")
    for i in cmps[0:10]:
        post, cmp_prof = i
        fout.write('<a href="./%d.html">%s</a> - %.6f <br/>\n' % (post, nouns[post], cmp_prof))
    
    fout.write(u"<h3>Профиль ответов на пост</h3>")
    profile_parts = reversed(sorted(profile.keys(), key=lambda x: float(profile[x])))
    
    for reply in profile_parts:
        p = profile[reply]    
        reply_text = nouns[reply] if reply != 0 else u"<i>шум</i>"
        fout.write('<a href="./%d.html">%s</a> - %.6f <br/>\n' % (reply, reply_text, p))

    footer = """
</body></html>
"""
    fout.write(footer)    
    fout.close() 

def write_noun_sim_index(posts, nouns, top_sims):
    filename = "./sim-net/index.html"
    print "[%s] write file %s " % (time.ctime(), filename)
    fout = codecs.open(filename,'w',encoding='utf8')
    fout.write(u"<html><head><meta charset=\"UTF-8\"><title>Индекс словопостов</title></head><body>\n<table border=\"1\">")

    heading = """
<html><head><meta charset=\"UTF-8\"></head><body>
"""   
    fout.write(heading)

    fout.write(u"<h3>Индекс словопостов<h3>\n")

    top_sims2 = map(lambda x: (x, top_sims[x][0], top_sims[x][1]), posts)
    top_sims2 = sorted(top_sims2, key=lambda x: x[2])

    for t in top_sims2:
        post1, post2, cmp_prof = t
        fout.write('<a href="./%d.html">%s</a>: %s - %s<br/>\n' % (post1, nouns[post1], nouns[post2], cmp_prof))

    footer = """
</body></html>
"""
    fout.write(footer)    
    fout.close() 


def main():
    print "[%s] Startup " % (time.ctime())
    con = sqlite3.connect(db)
    con.isolation_level = None
    
    cur = con.cursor()
    nouns = get_nouns(cur)

    abs_stats = get_noun_profiles(cur)
    #noise_stats = get_noun_profiles_noise(cur)
    total_stats = get_noun_profiles_total(cur)
    #post_cnt = get_post_cnt(cur)
    rel_stats = get_rel_stats(abs_stats, total_stats)

    post_tweets_cnt = get_post_tweets_cnt(cur)

    noise_cnt = 0
    for post in rel_stats:
        check_stats_row(post, rel_stats[post])  
        if 0 in rel_stats[post] and rel_stats[post][0] >= 0.5:
            noise_cnt += 1

    print "Total: %s; noise: %s" % (len(rel_stats), noise_cnt)

    #get_sim_map(rel_stats)

    top_sims = debug_sim_net(rel_stats, nouns, post_tweets_cnt)
    write_noun_sim_index(rel_stats.keys(), nouns, top_sims)

    print "[%s] Done " % (time.ctime())

if __name__ == '__main__':
    main()
