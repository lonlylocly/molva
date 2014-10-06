#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys
import os
import logging, logging.config
import json
from datetime import datetime
import signal

import stats
from Indexer import Indexer
import util
import KMeanCluster
import aligner

logging.config.fileConfig("logging.conf")

POST_MIN_FREQ = 10

settings = {} 
try:
    settings = json.load(open('global-settings.json', 'r'))
except Exception as e:
    logging.warn(e)

DB_DIR = settings["db_dir"] if "db_dir" in settings else os.environ["MOLVA_DIR"]

def handler(signum, frame):
    logging.error('Signal handler called with signal %s' % signum)
    raise Exception("Failed")

def get_sims(cur):
    res = cur.execute("select post1_md5, post2_md5, sim from noun_similarity")

    sim_dict = {}
    while True:
        r = cur.fetchone()
        if r is None:
            break
        p1, p2, sim = r
        if p1 not in sim_dict:
            sim_dict[p1] = {}
        if p2 not in sim_dict:
            sim_dict[p2] = {}
        sim_dict[p1][p2] = sim
        sim_dict[p2][p1] = sim
     
    for p in sim_dict.keys():
        sim_dict[p][p] = 0

    return sim_dict

def get_used_nouns(cur):
    res = cur.execute("""
        select post1_md5
        from noun_similarity
        group by post1_md5
        union
        select post2_md5
        from noun_similarity
        group by post2_md5
    """).fetchall()

    return map(lambda x: x[0], res)    

@util.time_logger
def get_clusters(args, sim_dict, nouns, noun_trend, post_cnt):
    best_ratio = 10 
    cl = []
    for k in [900, 1000, 1100]:
        for i in range(0, int(args.i)): 
            logging.info("get %s clusters, iteration %s" % (k, i))
            resp = KMeanCluster.get_clusters(sim_dict, int(k), nouns)
            ratio = resp["intra_dist"] / resp["extra_dist"]
            if (ratio) < best_ratio:
                best_ratio = ratio
                cl = resp["clusters"]
   
    logging.info("Best ratio: %s" % best_ratio) 
    for c in cl:
        for m in c["members"]:
            m["post_cnt"] = post_cnt[m["id"]]
            trend = noun_trend[m["id"]] if m["id"] in noun_trend else 0
            m["trend"] = "%.3f" % trend 

    return cl

 
def main():
    logging.info("start")
    parser = util.get_dates_range_parser()
    #parser.add_argument("-k")
    parser.add_argument("-i")
    args = parser.parse_args()

    today = (datetime.utcnow()).strftime("%Y%m%d%H%M%S")
    update_time = (datetime.now()).strftime("%Y-%m-%d %H:%M:%S")

    ind = Indexer(DB_DIR)

    cur = stats.get_main_cursor(DB_DIR)
    cur_lemma = stats.get_cursor(DB_DIR + "/tweets_lemma.db")
    cur_display = stats.get_cursor(DB_DIR + "/tweets_display.db")
    #cur.execute("drop table if exists clusters")

    stats.create_given_tables(cur_display, ["clusters"])

    #cnt = cur.execute("select count(*) from noun_similarity").fetchone()[0]

    used_nouns = get_used_nouns(cur)        
    logging.info("used nouns %s" % used_nouns)

    nouns = stats.get_nouns(cur, used_nouns)
    noun_trend = stats.get_noun_trend(cur)
    logging.info("nouns len %s" % len(nouns))
    post_cnt = stats.get_noun_cnt(cur)
    
    logging.info("get sim_dict")
    sim_dict = get_sims(cur) 

    cl = get_clusters(args, sim_dict, nouns, noun_trend, post_cnt)

    signal.signal(signal.SIGALRM, handler)
    signal.alarm(1200)
    cl = aligner.get_aligned_cluster(cur, cur_lemma, cl)
    signal.alarm(0)
    
    cl = {"clusters": cl, "update_time": update_time}
    cl_json = json.dumps(cl)
    cur_display.execute("""
        replace into clusters (cluster_date, cluster)
        values (?, ?)
    """, (today, cl_json))

    logging.info("Done")

if __name__ == '__main__':
    main()


