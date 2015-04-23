#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys
import os
import logging, logging.config
import json
from datetime import datetime

import molva.stats as stats
from Indexer import Indexer
import molva.util as util
import molva.KMeanCluster as KMeanCluster

logging.config.fileConfig("logging.conf")

POST_MIN_FREQ = 10

settings = {} 
try:
    settings = json.load(open('global-settings.json', 'r'))
except Exception as e:
    logging.warn(e)

DB_DIR = settings["db_dir"] if "db_dir" in settings else os.environ["MOLVA_DIR"]


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

@util.time_logger
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
    trash_words_md5 = map(util.digest, settings["trash_words"])
    best_ratio = 10 
    cl = []
    for k in [900, 1000, 1100]:
        for i in range(0, int(args.i)): 
            logging.info("get %s clusters, iteration %s" % (k, i))
            resp = KMeanCluster.get_clusters(sim_dict, int(k), nouns, trash_words=trash_words_md5)
            ratio = resp["intra_dist"] / resp["extra_dist"]
            if (ratio) < best_ratio:
                best_ratio = ratio
                cl = resp["clusters"]
   
    logging.info("Best ratio: %s" % best_ratio) 
    logging.info("Best clusters size: %s" % len(cl)) 
    for c in cl:
        for m in c["members"]:
            try:
                m["post_cnt"] = post_cnt[m["id"]]
            except Exception as e:
                logging.info("Mess with noun_md5 %s (%s)" % (m["id"], type(m["id"])))
                logging.error(e)
            trend = noun_trend[m["id"]] if m["id"] in noun_trend else 0
            m["trend"] = "%.3f" % trend 
   
    return util.filter_trash_words_cluster(cl)

 
def main():
    logging.info("start")
    parser = util.get_dates_range_parser()
    parser.add_argument("-i")
    args = parser.parse_args()

    ind = Indexer(DB_DIR)

    cur = stats.get_main_cursor(DB_DIR)
    cur_word_cnt = stats.get_cursor(DB_DIR + "/word_cnt.db")
    words_db = DB_DIR + "/tweets_lemma.db"
    bigram_db = DB_DIR + "/tweets_bigram.db"

    used_nouns = get_used_nouns(cur)        

    total_md5 = util.digest("__total__")

    nouns = stats.get_nouns(cur, used_nouns)
    noun_trend = stats.get_noun_trend(cur)
    nouns[total_md5] = "__total__"
    noun_trend["__total__"] = 0.0  
    logging.info("nouns len %s" % len(nouns))
    post_cnt = stats.get_noun_cnt(cur_word_cnt)
    post_cnt[total_md5] = 0
    
    logging.info("get sim_dict")
    sim_dict = get_sims(cur) 

    cl = get_clusters(args, sim_dict, nouns, noun_trend, post_cnt)

    json.dump(cl, open("./clusters_raw.json","w"), indent=2)

    logging.info("Done")

if __name__ == '__main__':
    main()


