#!/usr/bin/python
import httplib, urllib
import base64
import json
import sqlite3
import re
import time
from sets import Set
import sys,codecs
from datetime import datetime, timedelta
import os

sys.stdout = codecs.getwriter('utf8')(sys.stdout)

headers = {"Authorization":"Bearer AAAAAAAAAAAAAAAAAAAAAEBgVAAAAAAAxZcUIQhxg"+
"3gWnlJBMHUJ%2FSYIwbc%3DuIu1L6qBROC2wnONa37BVjw0z35FbjJSL2XuXa8fuCUc8wAWJW"}

#c.request('GET', '/1.1/search/tweets.json?%s'%params, '', headers)
#c.request('GET', '/1.1/statuses/show.json?id=%s'%tweet_id, '', headers)

CHAINS_GOAL = 10000000
#TWEETS_START_DAY = datetime(20114, 12, 25, 0, 0, 0) #datetime.now() - timedelta (days = 40)
#TWEETS_START_DAY = datetime.now() - timedelta (days = 2)
DB_FILENAME = os.environ["MOLVA_DB"] 

def get_tweet_start_time():
    return datetime.now() - timedelta (days = 2)

class WoapeException(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

def create_tables(cur):
    cur.execute("""
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
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users
        (
            username text,
            user_done integer default 0,
            reply_cnt integer default 0,
            PRIMARY KEY (username)
        )
    """)

def save_tweet(cur, reply, username):
    try:
        cur.execute("""
            INSERT OR IGNORE INTO tweets
            VALUES (?, ?, ?, ?, ?, ?)
        """, (reply["id"], reply["text"], username, reply["in_reply_to_screen_name"], reply["in_reply_to_status_id"], reply["created_at"]))
    except Exception as e:
        print "[ERROR] %s "  % e

def get_path(path):
    c = httplib.HTTPSConnection('api.twitter.com')
    c.set_debuglevel(0)
    print "[%s] Open path: %s" % (time.ctime(), path)
    resp = c.request('GET', path, '', headers)
    return c.getresponse()

def get_tweet_text(tweet_id) :
    path = '/1.1/statuses/show.json?id=%s'%tweet_id
    resp = get_path(path)
    if resp is not None:
        s = json.loads(resp.read())
        if "text" in s:
            return s["text"]
    return None

def got_russian_letters(s):
    res = re.match(u".*[а-яА-Я]+.*", s) is not None
    #print "got russian letters: %s %s" % (res, s)
    return res

def fetch_list(cur, query):
    return map(lambda x: x[0], cur.execute(query).fetchall())

def get_chains(cur):
    ids = fetch_list(cur, "select id from tweets")
    in_reply_to_ids = fetch_list(cur, "select in_reply_to_id from tweets")
    s1 = Set(ids)
    s2 = Set(in_reply_to_ids)
    chains = s1 & s2    
    return chains

def mark_user_done(cur, username):
    print "%s" % username
    cur.execute("update users set user_done = 1 where username = ?", (username, ))

def try_several_times(f, times, error_return=[]):
    tries = 0
    while tries < times:
        try:
            res = f()
            return res
        except WoapeException as e:
            print "[%s] Stop trying, WoapeException: %s" % (time.ctime(), e)
            break
        except Exception as e:
            print "[%s] [ERROR] %s" % (time.ctime(), e)
    return error_return

def get_tw_create_time(t):
    create_time = t["created_at"]
    create_time = re.sub("[+-]\d\d\d\d", "", create_time)
    dt = datetime.strptime(create_time, "%a %b %d %H:%M:%S  %Y")
    
    return dt

def get_more(cur, username, maxid=None):
    resp = None
    if maxid is None:
        resp = get_path('/1.1/statuses/user_timeline.json?screen_name=%s&count=200'%username)
    else:
        resp = get_path('/1.1/statuses/user_timeline.json?screen_name=%s&max_id=%s&count=200'%(username,maxid))
    if resp.status is not 200:
        print "[ERROR] Response code: %s %s. Response body: %s"% (resp.status, resp.reason, resp.read() )
        mark_user_done(cur, username)
        raise WoapeException('Invalid response')        

    return resp


def iteration(cur, username, replys_only=False):
    if username is None:
        raise WoapeException('Username is none')

    users_seen = fetch_list(cur, "select username from users") 
    new_users = []
    print "[%s] Start loop iteration: %s" % (time.ctime(), username)

    oldest_tweet_time = datetime.now()
    minid = None
    cnt = 0

    while oldest_tweet_time > get_tweet_start_time():
        resp = get_more(cur, username, minid) 

        content = resp.read()
        last_minid = minid

        conv_partners = {}
        for t in json.loads(content):
            ct = get_tw_create_time(t) 
            if ct < oldest_tweet_time:
                oldest_tweet_time = ct
            if ct < get_tweet_start_time():
                oldest_tweet_time = ct
                break
            if replys_only and t["in_reply_to_status_id"] is None :
                continue
            if not got_russian_letters(t["text"]):
                continue
            if minid is None or minid > int(t["id"]):
                minid = int(t["id"])
 
            #print "Got reply-tweet: %s '%s'" % (t["id"], t["text"])
            save_tweet(cur, t, username)
            cnt = cnt + 1

            fellow = t["in_reply_to_screen_name"]
            if fellow not in conv_partners:
                conv_partners[fellow] = 0
            conv_partners[fellow] = conv_partners[fellow] + 1
            if fellow is not None and fellow not in users_seen and fellow not in new_users:
                cur.execute("insert or ignore into users (username) values (?)", (fellow, ))
                new_users.append(fellow)

        for partner in conv_partners:
            cur.execute("update users set reply_cnt = reply_cnt + ? where username = ?", (conv_partners[partner], partner))

        if minid is None or minid == last_minid:
            print "[%s] cannot get valid minid" % (time.ctime())
            break    
    mark_user_done(cur, username)

    print "[%s] Oldest tweet time: %s " % (time.ctime(), oldest_tweet_time)
    
    print "Saved %s tweets" % cnt

    return new_users
   

def main():
    print "[%s] Startup" % time.ctime()
    con = sqlite3.connect(DB_FILENAME)
    con.isolation_level = None

    cur = con.cursor()
    create_tables(cur)

    cnt = 1
    cur.execute("insert or ignore into users values ('incubos', 0, 0) ")
    while True:
        users = fetch_list(cur, "select username from users where user_done = 0 order by reply_cnt desc limit 1")
    
        if len(users) == 0:
            print "No users left"
            break 
        username = users[0]
        users = users[1:]
   
        f = lambda :  iteration(cur, username)
        talked_to_users = try_several_times(f, 3, [])
       
        # try best to get full chains  
        #for talked_to_user in talked_to_users:
        #    if talked_to_user == username:
        #        continue
        #    f2 = lambda : iteration(cur, talked_to_user)
        #    try_several_times(f2, 3)
        cnt = cnt + 1
        if (cnt % 10) == 1:
            chains = get_chains(cur)     
            if len(chains) >= CHAINS_GOAL:
                print "Got enough chains, breaking"
                break
            else:
                print "Has %s chains, need %s, continue" % (len(chains), CHAINS_GOAL)


    
if __name__ == "__main__":
    main()
