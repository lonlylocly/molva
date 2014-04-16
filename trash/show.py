#!/usr/bin/python
import sqlite3
from sets import Set
import sys,codecs
import time

sys.stdout = codecs.getwriter('utf8')(sys.stdout)

def fetch_list(cur, query):
    return map(lambda x: x[0], cur.execute(query).fetchall())

def get_chains(cur):
    ids = fetch_list(cur, "select id from tweets")
    in_reply_to_ids = fetch_list(cur, "select in_reply_to_id from tweets")
    s1 = Set(ids)
    s2 = Set(in_reply_to_ids)
    chains = s1 & s2    
    return chains

def get_chains2(cur, index_file, tweets_file):
    tweets = cur.execute("select t.id, tw_text from tweets t inner join chains_count c on t.id = c.id")
    
    t = tweets.fetchone()
    cnt = 0
    while t is not None:
        index_file.write("%d\n" % t[0])
        cnt = cnt + 1
        tw_text = t[1]
        tw_text = tw_text.encode('utf-8')
        tw_text = tw_text.replace('\n', ' ').replace("'", "\\'")
        tweets_file.write("%s\n" % tw_text)
        t = tweets.fetchone()
        if cnt % 100000 == 0:
            print "[%s] %s tweets done" % (time.ctime(), cnt)


def main2():
    print "[%s] Startup" % time.ctime()
    con = sqlite3.connect('more_replys2.db')
    con.isolation_level = None

    cur = con.cursor()

    chains = get_chains2(cur)

    print "Has %s chains" % (len(chains))
    
    tweets = cur.execute("select count(*) from tweets").fetchone()[0]
    print "Has %s tweets" % tweets

    users = cur.execute("select count(*) from users where user_done =1 ").fetchone()[0]

    print "Has %s users" % users

def main():
    print "[%s] Startup" % time.ctime()
    con = sqlite3.connect('more_replys2.db')
    con.isolation_level = None

    cur = con.cursor()

    chains = get_chains2(cur, open('tweets_index.txt', 'w'), open('tweet_text.txt', 'w'))
   
if __name__ == "__main__":
    main()
