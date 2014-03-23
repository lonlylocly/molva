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
from stats import get_nouns, get_tweets_nouns, get_post_tweets, get_post_reply_tweets

sys.stdout = codecs.getwriter('utf8')(sys.stdout)

db = 'replys_sharper.db'

def get_tweet_link( tweet_id, username):
    link = "http://twitter.com/%s/status/%s" % (username, tweet_id)
    return "<a href=\"" + link + "\">" + link + "</a>"  

def get_tweet_links(tweets_usernames):
    links = []

    for t in tweets_usernames:
        links.append(get_tweet_link(t[0], t[1]))

    return links

def get_post_reply_tweets_links(cur, post_noun, reply_noun, tweets_nouns):
    tw_us = get_post_reply_tweets(cur, post_noun, reply_noun, tweets_nouns)

    return get_tweet_links(tw_us)
    
def get_post_tweets_links(cur, post_noun):
    tw_us = get_post_tweets(cur, post_noun)
    
    return get_tweet_links(tw_us)

def describe_noun(noun, noun_text, tweets_links):
    return """
    <div>
     %d <b>%s</b><br/>
     %s
    </div>
    """ % (int(noun), noun_text, "<br/>".join(tweets_links))

def get_f(filename):
    print "[%s] write %s " % (time.ctime(), filename)
    fout = codecs.open(filename,'w',encoding='utf8')
    fout.write("<html><head><meta charset=\"UTF-8\"></head><body>\n<table border=\"1\">")

    heading = """
<html><head><meta charset=\"UTF-8\"></head><body>\n<table border=\"1\">
"""   
    fout.write(heading)

    return fout


def close_f(fout):
    footer = """
</table></body></html>
"""
    fout.write(footer)    
    fout.close() 

    print "[%s] write file (done) " % (time.ctime())

def write_index(posts_replys, nouns):

    fout = get_f("reply-nouns/index.html")

    for post in posts_replys:
        ever_replys = len(posts_replys[post])
        post_text = nouns[int(post)]
        fout.write('<a href="./%s.html">%s</a> %d <br/>' % (post, post_text, ever_replys))
        #"<tr><td>"+"</td><td>"+"</td></tr>" 
    
    close_f(fout)    

def write_post_file(cur, post, post_replys, nouns, tweets_nouns):
    fout = get_f("reply-nouns/%s.html" % post)

    post_text = nouns[int(post)]
    p_tw = get_post_tweets_links(cur, int(post))
    desc = describe_noun(post, nouns[int(post)], p_tw)
    fout.write("<h3>post noun</h3>")
    fout.write(desc)
    fout.write("<h3>reply nouns</h3>")
    for reply in post_replys[post]:
        r_tw = get_post_reply_tweets_links(cur, post, reply, tweets_nouns)
        desc = describe_noun(reply, nouns[int(reply)], r_tw)
        fout.write(desc)

    close_f(fout)

def main():
    print "[%s] Startup " % (time.ctime())
    con = sqlite3.connect(db)
    con.isolation_level = None
    
    cur = con.cursor()
    nouns = get_nouns(cur)
    tweets_nouns = get_tweets_nouns(cur) 

    cur.execute("""
    CREATE TABLE IF NOT EXISTS post_reply_cnt ( 
        post_md5 integer, 
        reply_md5 integer, 
        post_cnt integer, 
        reply_cnt integer, 
        PRIMARY KEY(post_md5, reply_md5)
    )
    """)

    f = open("reply-noun-stat.json", "r")
    posts_replys = json.load(f)

    pr_cnt = {}
    for post in posts_replys:
        print "[%s] Operate noun %s " % (time.ctime(), post)
        p_tw = get_post_tweets(cur, int(post))
        post_tweets_len = len(p_tw)
        # 
        if post_tweets_len < 30:
            continue
        insert_data = []
        for reply in posts_replys[post]:
            r_tw = get_post_reply_tweets(cur, post, reply, tweets_nouns)   
            reply_tweets_len = len(r_tw)
            
            insert_data.append((post, reply, post_tweets_len, reply_tweets_len))

        cur.executemany("""
            insert or ignore into post_reply_cnt
            values (?, ?, ?, ?)
        """ , insert_data)

            

def main1():
    con = sqlite3.connect(db)
    con.isolation_level = None
    
    cur = con.cursor()
    nouns = get_nouns(cur)
    tweets_nouns = get_tweets_nouns(cur) 

    f = open("reply-noun-stat.json", "r")
    posts_replys = json.load(f)

    write_index(posts_replys, nouns)

    for post in posts_replys:
        write_post_file(cur, post, posts_replys, nouns, tweets_nouns)
    
if __name__ == '__main__':
    main()
