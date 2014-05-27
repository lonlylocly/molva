#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys
import os
import logging, logging.config
import json

import stats
from Indexer import Indexer
import util
import KMeanCluster

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
 
def main():
    parser = util.get_dates_range_parser()
    args = parser.parse_args()

    ind = Indexer(DB_DIR)

    cur_main = stats.get_cursor(DB_DIR + "/tweets.db")
    stats.create_given_tables(cur_main, ["clusters"])
    #cur_main.execute("create table if not exists clusters_svd as select * from clusters limit 0")

    for date in sorted(ind.dates_dbs.keys()):
        if args.start is not None and date < args.start:
            continue
        if args.end is not None and date > args.end:
            continue
        cur = ind.get_db_for_date(date)
      
        try:
            cur.execute("select 1 from noun_similarity")
        except Exception:
            logging.info("Skip date %s" % date)
            continue 

        cur.execute("drop table if exists clusters")

        cnt = cur.execute("select count(*) from noun_similarity").fetchone()
        cnt = cnt[0]

        logging.info("noun_similarity count: %s" % cnt)
        if cnt == 0:
            continue            

        nouns = stats.get_nouns(cur)
        logging.info("nouns len %s" % len(nouns))
        post_cnt = stats.get_noun_cnt(cur)
        
        logging.info("get sim_dict")
        sim_dict = get_sims(cur) 
        

        for k in [10, 100, 1000]: 
            logging.info("get clusters")
            cl = KMeanCluster.get_clusters(sim_dict, int(k), nouns)
            
            for c in cl:
                for m in c["members"]:
                    m["post_cnt"] = post_cnt[m["id"]]

            cl_json = json.dumps(cl)
            cur_main.execute("""
                replace into clusters (cluster_date, k, cluster)
                values (?, ?, ?)
            """, (date, k, cl_json))

    logging.info("Done")

if __name__ == '__main__':
    main()

