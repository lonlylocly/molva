#!/usr/bin/python
# -*- coding: utf-8 -*-
import sqlite3
from subprocess import Popen, PIPE, STDOUT
import sys,codecs
import re
import time
from util import digest
from subprocess import call
import os

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

    cnt = 0
    log_cnt = 0
    
    sim_f = open('_noun_sim_reduced.dump', 'r')

    sim_buffer = []
    sim_dict = {}
    while True:
        l = sim_f.readline()
        if l is not None and l != '':
            n1, n2, sim = l.split("\t")        
            if n1 in sim_dict:
                sim_dict[n1][n2] = float(sim)
            else:
                sim_dict[n1] = {n2: float(sim)}
            #sim_buffer.append((n1, n2, float(sim)))

        #if len(sim_buffer) == 0:
        #    break

        #if len(sim_buffer) >= 1000:
        #    cur.executemany("""
        #        insert or ignore into noun_similar
        #        (noun1_md5, noun2_md5, similar)
        #        values
        #        (?, ?, ?)
        #    """, sim_buffer )
        #    sim_buffer = []
        
        cnt = cnt + 1 
        if cnt > log_cnt * 1e6:
            log_cnt = log_cnt + 1
            print "[%s] Done so far %s" %(time.ctime(), cnt)   
            print call(['cat', '/proc/%d/status' % os.getpid()])

    print "[%s] Count %s" %(time.ctime(), cnt)   

if __name__ == "__main__":
    main()
