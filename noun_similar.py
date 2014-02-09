#!/usr/bin/python
# -*- coding: utf-8 -*-
import sqlite3
from subprocess import Popen, PIPE, STDOUT
import sys,codecs
import re
import time
from util import digest

sys.stdout = codecs.getwriter('utf8')(sys.stdout)

db = 'more_replys2.db'

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

def _count_similarity(n1_replys, n2_replys):
    sup_cnt = len(n1_replys | n2_replys)
    int_cnt = len(n1_replys & n2_replys)

    return (0.0 + int_cnt) / sup_cnt 

def count_similarity(nouns_replys, n1, n2):
    n1_replys = set(nouns_replys[n1] if n1 in nouns_replys else [])
    n2_replys = set(nouns_replys[n2] if n2 in nouns_replys else [])

    similar = _count_similarity(n1_replys, n2_replys) 
    
    return similar

def main():
    print "[%s] Startup" % time.ctime() 

    con = sqlite3.connect(db)
    con.isolation_level = None
    
    cur = con.cursor()
    create_tables(cur)

    n1_last, n2_last = cur.execute("select n1_md5, n2_md5 from sim_progress").fetchone()

    cnt = 0
    nouns_replys, replys_nouns = get_nouns_replys(cur)

    print "[%s] Fetched dict" % time.ctime() 
    nouns = nouns_replys.keys()
    
    nouns.sort()

    print "[%s] Sorted nouns, len %s" % (time.ctime(), len(nouns))

    log_cnt = 0
    
    sim_f = open('_noun_sim.dump', 'w')

    for n1 in nouns:
        sim_buffer = []
        pos_sims = []
        for reply in nouns_replys[n1]:
            for n2 in replys_nouns[reply]:
                pos_sims.append(n2)
        for n2 in set(pos_sims):
            cnt = cnt + 1
            similar = count_similarity(nouns_replys, n1, n2)
            sim_buffer.append((str(n1), str(n2), str(similar)))
        str_sim = map(lambda x: "\t".join(x), sim_buffer)
        sim_f.write("\n".join(str_sim))
        sim_f.write("\n")
        if cnt > log_cnt * 1e6:
            print "[%s] Done so far %s" %(time.ctime(), cnt)   
            log_cnt = log_cnt + 1
        
        #cur.executemany("""
        #    insert or ignore into noun_similar
        #    (noun1_md5, noun2_md5, similar)
        #    values
        #    (?, ?, ?)
        #""", sim_buffer )
        
        #cnt = cnt + 1 
        #print "[%s] Saved %s of %s post nouns" %(time.ctime(), cnt, len(nouns))   
    print "[%s] Count %s" %(time.ctime(), cnt)   

if __name__ == "__main__":
    main()
