#!/usr/bin/python
# -*- coding: utf-8 -*-
import sqlite3
from subprocess import Popen, PIPE, STDOUT
import sys,codecs
import re
import hashlib
import time

sys.stdout = codecs.getwriter('utf8')(sys.stdout)

db = 'replys.db'

def create_tables(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS nouns (
            noun_md5 integer,
            noun text,
            PRIMARY KEY(noun_md5)
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS noun_relations (
            post_noun_md5 integer,
            reply_noun_md5 integer,
            count integer default 0,
            PRIMARY KEY(post_noun_md5, reply_noun_md5)
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS progress (
            fake_key integer,
            last_id integer,
            last_reply_id integer,
            PRIMARY KEY (fake_key)
        )
    """)
    cur.execute("insert or ignore into progress (fake_key, last_id, last_reply_id) values (0, 0, 0)")

def fetch_list(cur, query):
    return map(lambda x: x[0], cur.execute(query).fetchall())

def tomitize(s):
    s = s.replace('\n', ' ').replace("'", "\\'")
    p = Popen(["./tomita", "config.proto"], stdout=PIPE, stdin=PIPE, stderr=PIPE)
    facts = p.communicate(input=s.encode('utf-8'))[0]
    facts = facts.decode('utf-8')
    nouns = re.findall("^.*Noun = (.*)$", facts, flags= re.M) 
    nouns = map(lambda x: x.lower(), nouns)

    return nouns

def digest(s):
    large = int(hashlib.md5(s.encode('utf-8')).hexdigest(), 16)

    b1 = large & (2 ** 32 - 1)
    b2 = large >> 32 & (2 ** 32 - 1)
    b3 = large >> 64 & (2 ** 32 - 1)
    b4 = large >> 96 & (2 ** 32 - 1)
    small = b1 ^ b2 ^ b3 ^ b4

    return small

def save_nouns(cur, nouns):
    for n in nouns:
        md5 = digest(n) 
        cur.execute("insert or ignore into nouns (noun_md5, noun) values (?, ?)", (md5, n))

def save_relations(cur, post_nouns, reply_nouns):
    for p in post_nouns:
        for r in reply_nouns:
            pmd5 = digest(p) 
            rmd5 = digest(r)
            cur.execute("""
                insert or ignore into noun_relations
                (post_noun_md5, reply_noun_md5)
                values
                (?, ?)
            """, (pmd5, rmd5) )
            cur.execute("""
                update noun_relations
                set count = count + 1
                where post_noun_md5 = ? and reply_noun_md5 = ? 
            """, (pmd5, rmd5))

def show_text(cur):
    return cur.execute("""
        select r.count, n1.noun, n2.noun from noun_relations as r
        inner join nouns n1 on r.post_noun_md5 = n1.noun_md5
        inner join nouns n2 on r.reply_noun_md5 = n2.noun_md5
        where r.count > 3
    """).fetchall()

def get_post_nouns(cur, post_id):
    res = cur.execute("""
        select n.noun from nouns as n
        inner join tweets_nouns as t
        on t.noun_md5 = n.noun_md5
        where 
            t.id = ?
    """, (post_id,)).fetchall()
    
    return map(lambda x: x[0], res)

def main():
    print "[%s] Startup" % time.ctime() 

    con = sqlite3.connect(db)
    con.isolation_level = None
    
    cur = con.cursor()
    create_tables(cur)

    last_id, last_reply_id = cur.execute("select last_id, last_reply_id from progress").fetchone()

    chains = cur.execute("""
        select t1.id, t2.id 
        from tweets as t1
        inner join tweets as t2
        on t1.in_reply_to_id = t2.id
        where 
            t1.in_reply_to_id not Null
        and 
            t1.id > ?
        and
            t2.id > ?
        order by t1.id, t2.id  
    """, (last_id, last_reply_id)).fetchall()

    print "[%s] Done fetching chains: %s" % (time.ctime(), len(chains)) 
    cnt = 0
    for c in chains:
	#try:
        post_nouns = get_post_nouns(cur, c[1]) 
        reply_nouns = get_post_nouns(cur, c[0]) 
        
        if len(post_nouns) == 0 or len(reply_nouns) == 0:
            print "[%s] no nouns for: %s, %s"% (time.ctime(), c[1], c[0])
            continue
        save_relations(cur, post_nouns, reply_nouns)
        
        cnt = cnt + 1
        print "[%s] Done chain: (post id, reply id) (%s, %s)" % (time.ctime(), c[1], c[0])
        cur.execute("update progress set last_id = ?, last_reply_id = ?", c)
        
        
        print "[%s] Chains left: %s" % (time.ctime(), len(chains) - cnt) 
     	#except Exception as e:
	#    print "[%s] ERROR. chain: (%s, %s) error: %s" % (time.ctime(), c[0], c[1], e ) 

def strip(s):
    s = s.replace('\n', ' ').replace("'", "\\'")
    return s
 
def main2():
    print "[%s] Startup" % time.ctime() 

    con = sqlite3.connect(db)
    con.isolation_level = None
    
    cur = con.cursor()
    create_tables(cur)

    chains = cur.execute("""
        select t1.id, t2.id 
        from tweets as t1
        inner join tweets as t2
        on t1.in_reply_to_id = t2.id
        where 
            t1.in_reply_to_id not Null
        order by t1.id, t2.id  
    """).fetchall()

    print "[%s] Done fetching chains: %s" % (time.ctime(), len(chains)) 
    cnt = 0
    fp = open('posts.txt', 'w')
    fr = open('ids.txt', 'w')
    for c in chains:
	try:
            post = cur.execute("select tw_text from tweets where id = ?", (c[1],)).fetchone()
            reply = cur.execute("select tw_text from tweets where id = ?", (c[0],)).fetchone()
         
            if post is not None and reply is not None:
		
                fp.write(strip(post[0]).encode('utf-8') + "\n") 
                fp.write(strip(reply[0]).encode('utf-8') + "\n") 
		fr.write("%s\n" % c[1]) 
		fr.write("%s\n" % c[0]) 
                #print "post: %s\nreply: %s" % (post[0], reply[0])
                #post_nouns = tomitize(post[0])
                #reply_nouns = tomitize(reply[0])
                #if len(post_nouns) == 0 or len(reply_nouns) == 0:
                #    continue
                #save_nouns(cur, post_nouns + reply_nouns)
                #save_relations(cur, post_nouns, reply_nouns)
             
         
         
     	except Exception as e:
	    print "[%s] ERROR. chain: (%s, %s) error: %s" % (time.ctime(), c[0], c[1], e ) 
    fp.close()
    fr.close()

if __name__ == "__main__":
    main()
