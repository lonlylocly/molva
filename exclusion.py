#!/usr/bin/python
import sys
import os
import logging, logging.config
import json
import math

import stats
from Indexer import Indexer
import util
from profile import NounProfile

logging.config.fileConfig("logging.conf")

settings = {} 
try:
    settings = json.load(open('global-settings.json', 'r'))
except Exception as e:
    logging.warn(e)

DB_DIR = settings["db_dir"] if "db_dir" in settings else os.environ["MOLVA_DIR"]

#  raw_p[p_md5][r_md5].append(r_id)
#
def get_profile_excluded(raw_p, p1, p2):
    profile = NounProfile(p1)

    prof1 = raw_p[p1]
    prof2 = raw_p[p2]
    for reply in raw_p[p1]:
        r_ids1 = set(prof1[reply])
        r_ids2 = set(prof2[reply] if reply in raw_p[p2] else [] )
        r_cnt = len(list(r_ids1 - r_ids2))
        if r_cnt == 0:
            continue
        profile.replys[reply] = r_cnt
        
    return profile

def get_raw_profiles(cur, nouns):
    logging.info("start")

    cur.execute("""
        select p_md5, r_id, r_md5 from chains_nouns
        where p_md5 in (%s)
    """ % (",".join(map(str, nouns))))    
  
    raw_p = {}
    while True:
        row = cur.fetchone()
        if row is None:
            break
        p_md5, r_id, r_md5 = row
        
        if p_md5 not in raw_p:
            raw_p[p_md5] = {}
        if r_md5 not in raw_p[p_md5]:
            raw_p[p_md5][r_md5] = []
        raw_p[p_md5][r_md5].append(r_id)

    logging.info("done")
    return raw_p

def get_coocurence(post_nouns, nouns_freqs):
    logging.info("start")
    coocurence = {}
    
    for post_id in post_nouns:
        nouns = post_nouns[post_id]
        for i in range(0, len(nouns)):
            for j in range(i + 1, len(nouns)):
                n_i = nouns[i]
                n_j = nouns[j]
                if n_i < n_j:
                    n_i, n_j = (n_j, n_i)

                if n_i not in coocurence:
                    coocurence[n_i] = {}
                if n_j not in coocurence[n_i]:
                    coocurence[n_i][n_j] = 0 
                coocurence[n_i][n_j] += 1

    logging.info("coocurence done")

    have_overlap = 0
    nouns = []
    coocurence2 = {}
    for n1 in coocurence:
        for n2 in coocurence[n1]:
            cnt = coocurence[n1][n2]
            # if happens less than 1 of 10
            if cnt < nouns_freqs[n1] / 10 and cnt < nouns_freqs[n2] / 10:
                continue
            have_overlap += 1
            if n1 not in coocurence2:
                coocurence2[n1] = {}
            if n2 not in coocurence2[n1]:
                coocurence2[n1][n2] = cnt

            nouns.append(n1)
            nouns.append(n2)

    coocurence = coocurence2

    nouns = list(set(nouns))

    logging.info("Total pairs with overlap: %s" % have_overlap)         
    logging.info("Total nouns with overlap: %s" % len(nouns))

    return (coocurence, nouns)

def get_post_nouns(cur):
    logging.info("start")
    stats.create_given_tables(cur, ["tweet_chains"])
    cur.execute("""
        select t.post_id, n.noun_md5
        from tweet_chains t
        inner join tweets_nouns_cur n
        on t.post_id = n.id
        where n.noun_md5 in (
            select post1_md5 
            from noun_similarity
            group by post1_md5
            union
            select post2_md5 
            from noun_similarity
            group by post2_md5
        )
    """)
    
    post_nouns = {}
    nouns_freqs = {}
    while True:
        r = cur.fetchone()
        if r is None:
            break

        post_id, noun_md5 = r
        if post_id not in post_nouns:
            post_nouns[post_id] = []
        if noun_md5 not in nouns_freqs:
            nouns_freqs[noun_md5] = 0

        nouns_freqs[noun_md5] += 1
        post_nouns[post_id].append(noun_md5) 

    logging.info("done")
    return (post_nouns, nouns_freqs)

def save_corrected(cur, data):
    cur.execute("begin transaction")
    cur.executemany("insert or ignore into noun_sim_corrected values (?, ?, ?)", data)
    cur.execute("commit")

def build_sim_corrected(cur, coocurence, raw_p):
    logging.info("start")

    cur.execute("drop table if exists noun_sim_corrected")
    cur.execute("create table noun_sim_corrected as select * from noun_similarity limit 0")
    
    data = [] 
    for n1 in coocurence:
        if n1 not in raw_p:
            logging.warn("%s not in raw_p" % n1)
            continue

        for n2 in coocurence[n1]:
            if n2 not in raw_p:
                logging.warn("%s not in raw_p" % n2)
                continue

            prof1 = get_profile_excluded(raw_p, n1, n2) 
            prof2 = get_profile_excluded(raw_p, n2, n1) 
            sim = None

            if len(prof1.replys) == 0 or len(prof2.replys) == 0:
                sim = 1
            else:
                sim = prof1.compare_with(prof2).sim

            data.append((n1, n2, sim))

            if len(data) > 10000:
                logging.info("Another 10k seen")
                save_corrected(cur, data)
                data = []

    save_corrected(cur, data)

    logging.info("done")

def apply_corrections(cur):
    cur.execute("begin transaction")

    cur.execute("""
        drop table if exists noun_similarity_old
    """)

    cur.execute("""
        alter table noun_similarity rename to noun_similarity_old
    """)

    stats.create_given_tables(cur, ["noun_similarity"])
    cur.execute("""
        insert into noun_similarity 
        select * from noun_similarity_old
    """)

    cur.execute("""
        replace into noun_similarity
        select * from noun_sim_corrected
    """)
    
    cur.execute("commit")

def main():
    parser = util.get_dates_range_parser()
    args = parser.parse_args()

    ind = Indexer(DB_DIR)
    
    cur = stats.get_main_cursor(DB_DIR)

    post_nouns, nouns_freqs = get_post_nouns(cur)

    coocurence, nouns = get_coocurence(post_nouns, nouns_freqs)

    raw_p = get_raw_profiles(cur, nouns)

    build_sim_corrected(cur, coocurence, raw_p)

    apply_corrections(cur)

if __name__ == '__main__':
    main()
