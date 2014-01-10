#!/usr/bin/python
# -*- coding: utf-8 -*-
import sqlite3
from subprocess import Popen, PIPE
import sys,codecs
import re
import hashlib
import time

sys.stdout = codecs.getwriter('utf8')(sys.stdout)

db = 'replys_40k.db'

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

def show_text(cur):
    return cur.execute("""
        select r.count, n1.noun, n2.noun from noun_relations as r
        inner join nouns n1 on r.post_noun_md5 = n1.noun_md5
        inner join nouns n2 on r.reply_noun_md5 = n2.noun_md5
        where r.count > 3
    """).fetchall()

def fetch_list(cur, query):
    return map(lambda x: x[0], cur.execute(query).fetchall())

def tomitize(s):
    s = s.replace('\n', ' ').replace("'", "\\'")
    p = Popen("./tomita-mac config.proto 2>/dev/null", stdout=PIPE, stdin=PIPE, stderr=PIPE)
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

def main():
    print "[%s] Startup" % time.ctime() 

    con = sqlite3.connect(db)
    con.isolation_level = None
    
    cur = con.cursor()
    create_tables(cur)

    last_id, last_reply_id = cur.execute("select last_id, last_reply_id from progress").fetchone()

    chains = cur.execute("""
        select id, in_reply_to_id 
        from tweets 
        where 
            in_reply_to_id not Null
        and 
            id > ?
        and
            in_reply_to_id > ?
        order by id, in_reply_to_id 
    """, (last_id, last_reply_id)).fetchall()

    cnt = 0
    for c in chains:
        post = cur.execute("select tw_text from tweets where id = ?", (c[1],)).fetchone()
        reply = cur.execute("select tw_text from tweets where id = ?", (c[0],)).fetchone()

        if post is not None and reply is not None:
            #print "post: %s\nreply: %s" % (post[0], reply[0])
            post_nouns = tomitize(post[0])
            reply_nouns = tomitize(reply[0])
            if len(post_nouns) == 0 or len(reply_nouns) == 0:
                continue
            save_nouns(cur, post_nouns + reply_nouns)
            save_relations(cur, post_nouns, reply_nouns)

            cnt = cnt + 1
            print "[%s] Done chain: (post id, reply id) (%s, %s)" % (time.ctime(), c[1], c[0])
            cur.execute("update progress set last_id = ?, last_reply_id = ?", c)


                
if __name__ == "__main__":
    main()
