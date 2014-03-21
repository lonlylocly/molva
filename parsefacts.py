#!/usr/bin/python
# -*- coding: utf-8 -*-

import hashlib
import sqlite3
import time
import sys,codecs

import xml.etree.cElementTree as ElementTree

from util import digest

sys.stdout = codecs.getwriter('utf8')(sys.stdout)


db = 'replys_sharper.db'

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

    ids = open('tweets_index.txt', 'r').read().split("\n")
    print "Got ids"

    tree = ElementTree.iterparse(sys.argv[1], events = ('start', 'end'))
    cur_doc = None
    cur_nouns = []
    cnt = 1
    for event, elem in tree:
        if event == 'end':
            if elem.tag == 'document':
                post_id = ids[int(cur_doc) -1]
                #nouns = map(lambda x: x.decode('utf-8'), cur_nouns)
                nouns = map(lambda x: x.lower(), cur_nouns)
                save_nouns(cur, nouns)
                save_tweet_nouns(cur, post_id, nouns)
                cur_doc = None
                cur_nouns = []
                elem.clear()
            if elem.tag == 'Noun':
                cur_nouns.append(elem.attrib['val'])
        if event == 'start':
            if elem.tag == 'document':
                cur_doc = elem.attrib['di']
                if int(cur_doc) > cnt * 10000:
                    print "[%s] seen %s docid" % (time.ctime(), cur_doc)
                    cnt = cnt + 1
    return


if __name__ == "__main__":
    main()
