#!/usr/bin/python
# -*- coding: utf-8 -*-
import sqlite3
from subprocess import Popen, PIPE, STDOUT
import sys,codecs
import re
import hashlib
import time
import json
import random

import simdict
from util import digest

sys.stdout = codecs.getwriter('utf8')(sys.stdout)

db = 'more_replys2.db'

def get_common_tweets(n1, n2, ns_tw):
    return set(ns_tw[n1]) & set(ns_tw[n2])

def get_common_replys(n1, n2, nouns_replys, nouns):
    n1_r = nouns_replys[n1]
    n2_r = nouns_replys[n2]
    n12_r = set(n1_r) & set(n2_r)
    
    n12_r = list(n12_r)
    n12_r = map(lambda x: nouns[x] if x in nouns else str(x), n12_r)

    return n12_r


def get_tweets_nouns(cur):
    res = cur.execute("""
        select id, noun_md5 
        from tweets_nouns
    """).fetchall()
   
    ns_tw = {}
    for r in res:
        t_id, n = r
        n = int (n)
        t_id = int(t_id)
        if n not in ns_tw:
            ns_tw[n] = [t_id]
        else:
            ns_tw[n].append(t_id) 
    return ns_tw


def get_nouns_replys(cur):
    r = cur.execute("""
        select post_noun_md5, reply_noun_md5 
        from noun_relations
    """).fetchall()

    nouns_replys = {}
    replys_nouns = {}
    for i in r:
        post = i[0]
        reply = i[1]
        if post in nouns_replys:
            nouns_replys[post].append(reply)
        else:
            nouns_replys[post] = [reply]
        if reply in replys_nouns:
            replys_nouns[reply].append(post)
        else:
            replys_nouns[reply] = [post]

    return (nouns_replys, replys_nouns)



def get_nouns(cur):
    res = cur.execute("""
        select noun_md5, noun from nouns
    """ ).fetchall()
    
    nouns = {}
    for r in res:
        nouns[r[0]] = r[1]
   
    return nouns


def main(input_file,output_file):
    con = sqlite3.connect(db)
    con.isolation_level = None
    
    cur = con.cursor()
    nouns = get_nouns(cur)
    kls = json.loads(open(input_file, 'r').read())

    ns_tw = get_tweets_nouns(cur)
    nouns_replys, replys_nouns = get_nouns_replys(cur)

    print "[%s] build klusters dict" % time.ctime() 
    kls_nouns = {}
    for k in kls.keys():
        k_n = nouns[int(k)]
        k_n_nouns = {} 
        for i in kls[k]:
            if k == i:
                continue 
            t_ids = get_common_tweets(int(k), int(i), ns_tw)
            noun_text = nouns[int(i)]
            t_ids = list(t_ids)
            if len(t_ids) > 0:
                continue
            com_repl = get_common_replys(int(k), int(i), nouns_replys, nouns)
            repl = ",".join(com_repl)
            if repl not in k_n_nouns:
                k_n_nouns[repl] = []
            k_n_nouns[repl].append(noun_text)
        if len(k_n_nouns) > 0:
            kls_nouns[k_n] = k_n_nouns
    
    assoc = []
    for k in kls_nouns.keys():
        if k in [u'комментарий', u'дак']:
            continue 
        for repl in kls_nouns[k]:
            assoc.append((k, repl.split(","), kls_nouns[k][repl]))
    assoc = list(reversed(sorted(assoc, key=lambda x: len(x[1]))))

    write_assoc(output_file, assoc, cur, ns_tw)

def get_tweet_link(cur, tweet_id):
    res = cur.execute("select username from tweets where id = %s" % tweet_id).fetchone()
    
    username = res[0]
    link = "http://twitter.com/%s/status/%s" % (username, tweet_id)
    return link

def get_reply_tweets(cur, post_ids=[], reply_nouns=[]):
    reply_nouns_md5 = map(digest, reply_nouns)
    res = cur.execute("""
        select 
        tw.id
        from tweets_nouns tw
        inner join tweets t
        on tw.id = t.id
        where t.in_reply_to_id in (%s)
        and tw.noun_md5 in (%s)
        group by tw.id
    """ % (
        ",".join(map(str,post_ids)),
        ",".join(map(str,reply_nouns_md5))
        )
    ).fetchall()
    ids = map(lambda x: x[0], res)
    
    return ids


def get_reply_tweets2(cur, ns_tw, noun, reply_nouns):
    noun_md5 = digest(noun)

    tweets_with_nouns = ns_tw[noun_md5]
    reply_tweets_with_nouns = get_reply_tweets(cur, tweets_with_nouns, reply_nouns)

    return reply_tweets_with_nouns
 
def get_noun_tweet_links(cur, noun_tweets):
    tweet_links = map(lambda x: get_tweet_link(cur, x), noun_tweets) 

    return tweet_links   

def get_noun_usage_info(cur, ns_tw, noun, reply_nouns):

    noun_tweets = get_reply_tweets2(cur, ns_tw, noun, reply_nouns)
    tw_len = len(noun_tweets)
 
    tw_links = get_noun_tweet_links(cur, noun_tweets[0:10])
    tw_links = map(lambda x: "<a href=\"%s\">%s</a>" % (x, x), tw_links)
    tw_links = "<br/>\n".join(tw_links)
    return """
<div>
<b>%s</b> <br/>
len: %d <br/>
%s
</div>
""" % (noun, tw_len, tw_links)
 
def write_assoc(output_file, assoc, cur, ns_tw): 
    f = codecs.open(output_file,'w',encoding='utf8')
    f.write("<html><head><meta charset=\"UTF-8\"></head><body>\n<table border=\"1\">")
    for a in assoc[0:1000]:
        f.write("<tr>")
        f.write("<td>%s</td>"% (get_noun_usage_info(cur, ns_tw, a[0], a[1])))
        f.write("<td>(%s)</td>" % (", ".join(a[1])))
        f.write("<td>")
        print len(a[2])
        for i in a[2]:
            f.write(get_noun_usage_info(cur, ns_tw, i, a[1]))
        f.write("</td></tr>\n")
    f.write("</table></body></html>")
    f.close()

if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
