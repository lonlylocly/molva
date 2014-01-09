#!/usr/bin/python
import httplib, urllib
import base64
import json
import sqlite3
import re
import time
from sets import Set
import sys,codecs

sys.stdout = codecs.getwriter('utf8')(sys.stdout)

headers = {"Authorization":"Bearer AAAAAAAAAAAAAAAAAAAAAEBgVAAAAAAAxZcUIQhxg"+
"3gWnlJBMHUJ%2FSYIwbc%3DuIu1L6qBROC2wnONa37BVjw0z35FbjJSL2XuXa8fuCUc8wAWJW"}

#c.request('GET', '/1.1/search/tweets.json?%s'%params, '', headers)
#c.request('GET', '/1.1/statuses/show.json?id=%s'%tweet_id, '', headers)

CHAINS_GOAL = 1000000

def create_tables(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS tweets
        (
            id integer,
            tw_text text,
            username text,
            in_reply_to_username text,
            in_reply_to_id integer,
            PRIMARY KEY (id)
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users
        (
            username text,
            user_done integer default 0,
            PRIMARY KEY (username)
        )
    """)

def save_reply(cur, reply, username):
    try:
        cur.execute("""
            INSERT OR IGNORE INTO tweets
            VALUES (?, ?, ?, ?, ?)
        """, (reply["id"], reply["text"], username, reply["in_reply_to_screen_name"], reply["in_reply_to_status_id"]))
    except Exception as e:
        print "[ERROR] %s "  % e

def get_path(path):
    c = httplib.HTTPSConnection('api.twitter.com')
    c.set_debuglevel(0)
    print "Open path: %s" % path
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
        except Exception as e:
            print "[%s] [ERROR] %s" % (time.ctime(), e)
    return error_return

def iteration(cur, username, replys_only=False):
    if username is None:
        print "[ERROR] got None username"
        return []

    users_seen = fetch_list(cur, "select username from users") 
    new_users = []
    print "[%s] Start loop iteration: %s" % (time.ctime(), username)
    
    resp = get_path('/1.1/statuses/user_timeline.json?screen_name=%s&count=200'%username)
    if resp.status is not 200:
        print "[ERROR] Response code: %s %s. Response body: %s"% (resp.status, resp.reason, resp.read() )
        mark_user_done(cur, username)
        return []

    content = resp.read()

    cnt = 0
    for t in json.loads(content):
        if replys_only and t["in_reply_to_status_id"] is None :
            continue
        if not got_russian_letters(t["text"]):
            continue
        
        #print "Got reply-tweet: %s '%s'" % (t["id"], t["text"])
        save_reply(cur, t, username)
        cnt = cnt + 1

        fellow = t["in_reply_to_screen_name"]
        if fellow is not None and fellow not in users_seen and fellow not in new_users:
            cur.execute("insert or ignore into users (username) values (?)", (fellow, ))
            new_users.append(fellow)
    
    mark_user_done(cur, username)
    
    print "Saved %s tweets" % cnt

    return new_users
   

def main():
    print "[%s] Startup" % time.ctime()
    con = sqlite3.connect('replys.db')
    con.isolation_level = None

    cur = con.cursor()
    create_tables(cur)

    while True:
        users = fetch_list(cur, "select username from users where user_done = 0 limit 100")
    
        if len(users) == 0:
            print "No users left"
            break 
        username = users[0]
        users = users[1:]
   
        f = lambda :  iteration(cur, username)
        talked_to_users = try_several_times(f, 3, [])
       
        # try best to get full chains  
        for talked_to_user in talked_to_users:
            if talked_to_user == username:
                continue
            f2 = lambda : iteration(cur, talked_to_user)
            try_several_times(f2, 3)

        chains = get_chains(cur)     
        if len(chains) >= CHAINS_GOAL:
            print "Got enough chains, breaking"
            break


def old_main():
    while len(users) > 0 and len(users_done) < 100:
        username = users[0]
        print "Start loop iteration: %s" % username
        print users
        users_done.append(username)
        
        resp = get_path('/1.1/statuses/user_timeline.json?screen_name=%s&count=200'%username)
        if resp.status is not 200:
            print "[ERROR] Response code: %s %s. Response body: %s"% (resp.status, resp.reason, resp.read() )
            continue

        new_users = []
        content = resp.read()

        for t in json.loads(content):
            if t["in_reply_to_status_id"] is not None and got_russian_letters(t["text"]):
                print "Got reply-tweet: %s '%s'" % (t["id"], t["text"])
                save_reply(cur, t, username)
                con.commit()

                fellow = t["in_reply_to_screen_name"]
                if fellow not in users_seen:
                    new_users.append(fellow)
                    users_seen.append(fellow)
                    cur.execute("insert or ignore into users (username) values (?)", (fellow))
        
        users = users + new_users
        cur.execute("update users set user_done = 1 where username = ?", (username))
    
if __name__ == "__main__":
    main()
