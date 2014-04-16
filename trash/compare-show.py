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
import NounProfileStat

BASE_DIR = "./sim-net/compare-profiles/"

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


def main():
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
