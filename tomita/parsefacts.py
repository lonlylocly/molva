#!/usr/bin/python
# -*- coding: utf-8 -*-
import libxml2

import hashlib
import sqlite3
import time
import sys,codecs

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
    CREATE TABLE IF NOT EXISTS tweets_nouns(
	id integer,
	noun_md5 integer,
	PRIMARY KEY(id, noun_md5)
    )
    """) 

def digest(s):
    large = int(hashlib.md5(s.encode('utf-8')).hexdigest(), 16)

    b1 = large & (2 ** 32 - 1)
    b2 = large >> 32 & (2 ** 32 - 1)
    b3 = large >> 64 & (2 ** 32 - 1)
    b4 = large >> 96 & (2 ** 32 - 1)
    small = b1 ^ b2 ^ b3 ^ b4

    return small

def save_nouns(cur, nouns):
    cur.executemany("insert or ignore into nouns (noun_md5, noun) values (?, ?)", 
    map(lambda x: (digest(x), x), nouns ))

def save_tweet_nouns(cur, post_id, nouns):
    cur.executemany("insert or ignore into tweets_nouns (id, noun_md5) values (?, ?)", 
    map(lambda x: (post_id, digest(x)), nouns))


def main():
    con = sqlite3.connect(db)
    con.isolation_level = None
    
    cur = con.cursor()
    create_tables(cur)   

    doc = libxml2.parseFile('facts.xml')
    c = doc.xpathNewContext()
    res = c.xpathEval("//document")

    ids = open('ids.txt', 'r').read().split("\n")

    for d in res:
        c.setContextNode(d)
        doc_id = c.xpathEval("./@di")[0].content
        post_id = ids[int(doc_id) - 1]
        nouns = map(lambda x: x.content,  c.xpathEval("./facts/SimpleFact/Noun/@val"))
	nouns = map(lambda x: x.decode('utf-8'), nouns)
	nouns = map(lambda x: x.lower(), nouns)
	print "[%s] postId %s nouns %s" % (time.ctime(), post_id, ",".join(nouns))
	save_nouns(cur, nouns)
	save_tweet_nouns(cur, post_id, nouns)


if __name__ == "__main__":
    main()
