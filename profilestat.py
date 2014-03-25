#!/usr/bin/python
# -*- coding: utf-8 -*-
import sqlite3
import sys,codecs
import time
import math
import json
import re
import os

from stats import get_tweets_nouns, get_post_replys_tweets, get_nouns, get_post_tweets_cnt 
from util import digest

from profile import NounProfile

from bintrees import RBTree

sys.stdout = codecs.getwriter('utf8')(sys.stdout)

db = 'replys_sharper.db'

TIMESTAMP = time.strftime("%m_%d_%H_%M_%S", time.gmtime())

BASE_DIR = "./sim-net/ver7/"


REPLY_REL_MIN = 0.005
POST_MIN_FREQ = 30
REPLY_MIN_FREQ = 1 

BLOCKED_NOUNS_LIST = u""
BLOCKED_NOUNS_LIST2 = u"""
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

def get_noun_profiles(cur):
    stats = cur.execute("""
        select p.post_md5, p.reply_md5, p.reply_cnt
        from post_reply_cnt p
        inner join post_cnt p2
        on p.post_md5 = p2.post_md5
        where p2.post_cnt > %d
        and p.post_md5 not in (%s) 
        and p.reply_md5 not in (%s)
    """ % (POST_MIN_FREQ, BLOCKED_NOUNS, BLOCKED_NOUNS)).fetchall()

    profiles_dict = {}

    for s in stats:
        post = int(s[0])
        reply = int(s[1])
        cnt = int(s[2])
        if post not in profiles_dict:
            profiles_dict[post] = NounProfile(post, REPLY_REL_MIN) 
        profiles_dict[post].replys[reply] = cnt

    return profiles_dict

def set_noun_profiles_tweet_ids(profiles_dict, tweet_nouns):
    for x in profiles_dict.keys():
        profiles_dict[x].post_tweet_ids = tweet_nouns[x] 

def set_noun_profiles_total(cur, profiles_dict):
    total_stats = cur.execute("""
        select p.post_md5, sum(p.reply_cnt)
        from post_reply_cnt p
        inner join post_cnt p2
        on p.post_md5 = p2.post_md5
        where p2.post_cnt > %d
        and p.post_md5 not in (%s)
        and reply_md5 not in (%s)
        group by p.post_md5
    """ % (POST_MIN_FREQ, BLOCKED_NOUNS, BLOCKED_NOUNS)).fetchall()

    for s in total_stats:
        post = int(s[0])
        reply_cnt = int(s[1])

        #assert post in profiles_dict
        if post not in profiles_dict:
            continue

        profiles_dict[post].total = reply_cnt 


def set_rel_stats(profiles_dict):
    for post in profiles_dict:
        profiles_dict[post].setup_rel_profile() 
         
def debug_sim_net(profiles_dict, nouns, tweets_nouns):
    f = open("sim.txt", "w")

    posts = profiles_dict.keys()

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
            p_compare = profiles_dict[post1].compare_with(profiles_dict[post2])
            f.write("%d\t%d\t%f\n" % (p_compare.left.post, p_compare.right.post, p_compare.sim))
            cnt += 1
            cmps.append(p_compare)
        cmps = sorted(cmps, key=lambda x: x.sim)

        write_noun_sim_info(profiles_dict[post1], cmps, nouns, tweets_nouns)
        top_sims[post1] = cmps[0] 
    
    f.close()

    return top_sims

def get_common_tweet_ratio(post1, post2, tweet_nouns):
    t1 = tweet_nouns[post1]
    t2 = tweet_nouns[post2]

    common = set(t1) & set(t2)
    total = set(t1) | set(t2)

    return (len(common) + 0.0) / len(total)

def write_noun_sim_info(post_profile, cmps, nouns, tweets_nouns):
    post = post_profile.post
    profile = post_profile.replys_rel

    post_tweet_cnt = len(set(tweets_nouns[post]))
    filename = BASE_DIR + str(post) + ".html"
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
        post2 = i.right.post
        fout.write(u'<a href="./%d.html">%s</a> - %s<br/>\n' % (post2, nouns[post2], i))
    
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
    filename = BASE_DIR + "index.html"
    print "[%s] write file %s " % (time.ctime(), filename)
    fout = codecs.open(filename,'w',encoding='utf8')
    fout.write(u"<html><head><meta charset=\"UTF-8\"><title>Индекс словопостов</title></head><body>\n<table border=\"1\">")

    heading = """
<html><head><meta charset=\"UTF-8\"></head><body>
"""   
    fout.write(heading)

    fout.write(u"<h3>Индекс словопостов<h3>\n")

    top_sims2 = map(lambda x: (x, top_sims[x]), posts)
    top_sims2 = sorted(top_sims2, key=lambda x: x[1].sim)

    for t in top_sims2:
        post1, post2, cmp_prof = (t[0], t[1].right.post, t[1].sim)
        fout.write('<a href="./%d.html">%s</a>: %s - %s<br/>\n' % (post1, nouns[post1], nouns[post2], cmp_prof))

    footer = """
</body></html>
"""
    fout.write(footer)    
    fout.close() 

def setup_noun_profiles(cur, tweets_nouns):
    profiles_dict = get_noun_profiles(cur)

    set_noun_profiles_tweet_ids(profiles_dict, tweets_nouns)

    set_noun_profiles_total(cur, profiles_dict)

    set_rel_stats(profiles_dict)

    return profiles_dict

def main():
    os.mkdir(BASE_DIR)
    print "[%s] Startup " % (time.ctime())
    con = sqlite3.connect(db)
    con.isolation_level = None
    
    cur = con.cursor()
    nouns = get_nouns(cur)

    tweets_nouns = get_tweets_nouns(cur)

    profiles_dict = setup_profiles_dict(cur, tweets_nouns)
    
    top_sims = debug_sim_net(profiles_dict, nouns, tweets_nouns)

    write_noun_sim_index(profiles_dict.keys(), nouns, top_sims)

    print "[%s] Done " % (time.ctime())

if __name__ == '__main__':
    main()
