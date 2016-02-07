#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys
import os
import logging, logging.config
import json
from datetime import datetime

import molva.stats as stats
from molva.Indexer import Indexer
import molva.util as util

logging.config.fileConfig("logging.conf")

POST_MIN_FREQ = 10

settings = {} 
try:
    settings = json.load(open('global-settings.json', 'r'))
except Exception as e:
    logging.warn(e)

DB_DIR = settings["db_dir"] if "db_dir" in settings else os.environ["MOLVA_DIR"]

total_md5 = util.digest("__total__")


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

def find_knn(w, sims, l=8, threshold=0.5, do_print=True):
    ns = [ (x, sims[w][x]) for x in sims[w].keys() if w != x and sims[w][x] < threshold]
    ns = sorted(ns, key =lambda x: x[1])
    if do_print:
        for x in ns[:l]:
            print u"  %s %s"% (x[0], x[1]) 
    return ns[:l] 

def get_cluster_md5(cl):
    s = ",".join(map(str,cl)) 

    return util.digest_large(s)

def make_clusters_object(cl, nouns, noun_trend):
    cl2 = []
    avg_dist = 0.0
    for c in sorted(cl.keys(), key=lambda x: noun_trend[int(x)], reverse=True):
        logging.info(u"%s: %s" % (noun_trend[int(c)], " ".join([nouns[int(x)] for x in cl[c]])))
        struct = {  
            'members': [{'id': x, 'text': nouns[int(x)]} for x in cl[c]], 
            'members_len': len(cl[c]),
            'members_md5': str(get_cluster_md5(cl[c])),
            'avg_dist': "%.2f" % 0, # don't care
            'centroid_md5': str(c),
            'centroid_text': nouns[int(c)]
        }
        cl2.append(struct)
    for c in cl2:
        for m in c["members"]:
            m["post_cnt"] = 0 # stub 
            trend = noun_trend[m["id"]] if m["id"] in noun_trend else 0
            m["trend"] = "%.3f" % trend 

    return cl2

@util.time_logger
def get_clusters(sim_dict, nouns, noun_trend):
    trash_words_md5 = map(util.digest, settings["trash_words"])
    
    trendy = [ x for x in sim_dict.keys() if x in noun_trend ]
    trendy = sorted(trendy, key=lambda x: noun_trend[x], reverse=True)
    clusters = {}
    non_centroids = {total_md5: 1}    
    # threshold to filter trashlike from being centroids
    l = find_knn(total_md5, sim_dict, l=2000, threshold=settings["knn_trash_threshold"], do_print=False)
    for x in l:
        non_centroids[x[1]] = 1
    for t in trendy:
        if t in non_centroids:
            continue
        # topic threshold
        l = find_knn(t, sim_dict, l=8, threshold=settings["knn_neighbors_threshold"], do_print=False)
        if len(l) == 0:
            continue
        for x in l:
            non_centroids[x[0]] = 1
        members = [t] + [x[0] for x in l ] 
        # __total__ in members sinks entire cluster
        if len([x for x in members if x == total_md5]) == 0 and len(members) > 1:
            clusters[t] = members

    return make_clusters_object(clusters, nouns, noun_trend)

@util.time_logger 
def main():
    logging.info("start")
    parser = util.get_dates_range_parser()
    args = parser.parse_args()

    ind = Indexer(DB_DIR)

    cur = stats.get_main_cursor(DB_DIR)
    cur_word_cnt = stats.get_cursor(DB_DIR + "/word_cnt.db")

    used_nouns = get_used_nouns(cur)        

    nouns = stats.get_nouns(cur, used_nouns)
    noun_trend = stats.get_noun_trend(cur)
    nouns[total_md5] = "__total__"
    noun_trend[total_md5] = 0.0  
    logging.info("nouns len %s" % len(nouns))
    
    logging.info("get sim_dict")
    sim_dict = get_sims(cur) 

    cl = get_clusters(sim_dict, nouns, noun_trend)

    json.dump(cl, open("./clusters_raw.json","w"), indent=2)

    logging.info("Done")

if __name__ == '__main__':
    main()


