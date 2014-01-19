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
        CREATE TABLE IF NOT EXISTS noun_similar (
            noun1_md5 integer,
            noun2_md5 integer,
            similar float default 0,
            PRIMARY KEY(noun1_md5, noun2_md5)
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sim_progress (
            fake_key integer,
            n1_md5 integer,
            n2_md5 integer,
            PRIMARY KEY (fake_key)
        )
    """)
    cur.execute("insert or ignore into sim_progress (fake_key, n1_md5, n2_md5) values (0, 0, 0)")

def fetch_list(cur, query):
    return map(lambda x: x[0], cur.execute(query).fetchall())

def digest(s):
    large = int(hashlib.md5(s.encode('utf-8')).hexdigest(), 16)

    b1 = large & (2 ** 32 - 1)
    b2 = large >> 32 & (2 ** 32 - 1)
    b3 = large >> 64 & (2 ** 32 - 1)
    b4 = large >> 96 & (2 ** 32 - 1)
    small = b1 ^ b2 ^ b3 ^ b4

    return small

def get_nouns_replys(cur, reverse=False):
    r = cur.execute("""
        select post_noun_md5, reply_noun_md5 
        from noun_relations
    """).fetchall()

    nouns_replys = {}
    for i in r:
        post = i[0 if not reverse else 1]
        reply = i[1 if not reverse else 1]
        if post in nouns_replys:
            nouns_replys[post].append(reply)
        else:
            nouns_replys[post] = [reply]

    return nouns_replys

def _count_similarity(n1_replys, n2_replys):
    sup_cnt = len(n1_replys | n2_replys)
    int_cnt = len(n1_replys & n2_replys)

    return (0.0 + int_cnt) / sup_cnt 

def count_similarity(nouns_replys, n1, n2):
    n1_replys = set(nouns_replys[n1] if n1 in nouns_replys else [])
    n2_replys = set(nouns_replys[n2] if n2 in nouns_replys else [])

    similar = _count_similarity(n1_replys, n2_replys) 
    
    return similar

def save_similarity(cur, n1, n2, similar):
    cur.execute("""
        insert or ignore into noun_similar
        (noun1_md5, noun2_md5, similar)
        values
        (?, ?, ?)
    """, (n1, n2, similar))
         
def main2():
    print "[%s] Startup" % time.ctime() 

    con = sqlite3.connect(db)
    con.isolation_level = None
    
    cur = con.cursor()
    create_tables(cur)

    n1_last, n2_last = cur.execute("select n1_md5, n2_md5 from sim_progress").fetchone()

    nouns = cur.execute("""
        select noun_md5 from nouns
        where noun_md5 > ?
        order by noun_md5
    """, (n1_last, )).fetchall()
    nouns = map(lambda x: x[0], nouns)

    cnt = 0
    nouns_replys = get_nouns_replys(cur)
    replys_nouns = get_nouns_replys(cur, reverse=True)

    nouns = nouns_replys.keys()

    print "Nouns len: %s" % len(nouns)

    for n1 in nouns_replys.keys():
        sim_buffer = []
        similar_candidates = []
        for r in nouns_replys[n1]:
            if r not in replys_nouns:
                continue
            for n2 in replys_nouns[r]:
                similar_candidates.append(n2)
        similar_candidates = set(similar_candidates)
        similar_candidates -= set([n1])

        print "[%s] For noun '%s' similar candidates: %s" % (time.ctime(), n1, similar_candidates)
        for n2 in similar_candidates:
            similar = count_similarity(nouns_replys, n1, n2)
            sim_buffer.append((n1, n2, similar))

        cur.executemany("""
            insert or ignore into noun_similar
            (noun1_md5, noun2_md5, similar)
            values
            (?, ?, ?)
        """, sim_buffer )
        
        cnt = cnt + 1 
        print "[%s] Saved %s of %s post nouns" %(time.ctime(), cnt, len(nouns))   


if __name__ == "__main__":
    main2()
