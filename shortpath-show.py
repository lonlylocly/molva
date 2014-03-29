#!/usr/bin/python
# -*- coding: utf-8 -*-

import time
import stats
import codecs
import json
import os

import shortpath
import profilestat
from stats import *

BASE_DIR = "./sim-net/shortpath/"

def write_index(g, nouns):
    filename = BASE_DIR + "index.html"
    print "[%s] write file %s " % (time.ctime(), filename)
    fout = codecs.open(filename,'w',encoding='utf8')
    fout.write(u"<html><head><meta charset=\"UTF-8\"><title>Индекс словопостов</title></head><body>\n<table border=\"1\">")

    heading = """
<html><head><meta charset=\"UTF-8\"></head><body>
"""   
    fout.write(heading)

    fout.write(u"<h3>Индекс словопостов<h3>\n")

    ll = [] 
    ps = map(lambda x: x.p1, g)
    ps += map(lambda x: x.p2, g)

    ps = set(ps)
    for noun in ps:
        l = filter(lambda e: e.p1 == noun or e.p2 == noun, g)
        ll.append((noun, l))
    fout.write(u"<table border=1><tr><th>Словопост</th><th>Всего похожих</th><th>Самый похожий</th></tr>")

    ll = sorted(ll, key=lambda x: len(x[1]), reverse=True)
    for l in ll:
        if l[1] == 0:
            continue
        post1 = l[0]
        edges = sorted(l[1], key=lambda x: x.s)
        edge = edges[0]
        post2 = edge.p1 if edge.p1 != post1 else edge.p2
        if edge.s > 0.5:
            continue
        fout.write(u'<tr><td><a href="./%d.html">%s</a></td><td>%d</td><td>%s</td><td>%s</td></tr>\n' % (post1, nouns[post1], len(edges), nouns[post2], edge.s))

    fout.write(u"</table>")
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


def write_noun_info(post_profile, graph, nouns, tweets_nouns):

    post = post_profile.post 

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
    fout.write(u"<h3>Наиболее похожие посты</h3>")
    for i in filter(lambda x: x.p1 == post or x.p2 == post, graph):
        post2 = i.p2 if i.p1 == post else i.p1
        fout.write(u'<a href="./%d.html">%s</a> - %s<br/>\n' % (post2, nouns[post2], i.s))
    
    fout.write(u"<h3>Профиль ответов на пост</h3>")
    profile_parts = reversed(sorted(post_profile.replys_rel.keys(), key=lambda x: float(post_profile.replys_rel[x])))
    
    for reply in profile_parts:
        p = post_profile.replys_rel[reply]    
        reply_text = nouns[reply] if reply != 0 else u"<i>шум</i>"
        fout.write('<a href="./%d.html">%s</a> - %.6f <br/>\n' % (reply, reply_text, p))

    footer = """
</body></html>
"""
    fout.write(footer)    
    fout.close() 

def main():
    #os.mkdir(BASE_DIR)
    print "[%s] Startup " % (time.ctime())

    cur = stats.get_cursor("replys_sharper.db")   
    nouns = stats.get_nouns(cur)
 
    fin = codecs.open("graph.txt", "r",encoding='utf8')
    g = shortpath.read_json(fin) 
   
    nouns = get_nouns(cur)

    tweets_nouns = get_tweets_nouns(cur)

    profiles_dict = profilestat.setup_noun_profiles(cur, tweets_nouns)
  
    for i in profiles_dict:
        post_profile = profiles_dict[i]
        write_noun_info(post_profile, g, nouns, tweets_nouns)
    f = codecs.open("graph-stats.txt", "w",encoding='utf8')

    write_index(g, nouns)

    #f.close()
    print "[%s] Done " % (time.ctime())


if __name__ == '__main__':
    main()
