#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys,codecs
import time
import json
import os
import random
import logging

import stats
import util

sys.stdout = codecs.getwriter('utf8')(sys.stdout)

def get_best_cluster(post, clusters, sim_dict):
    cluster_sims = map( lambda x: (x, sim_dict[post][x]), clusters.keys())
    best_cluster = sorted(cluster_sims, key= lambda x: x[1])[0][0]

    return best_cluster

def iteration(clusters, sim_dict):
    new_clusters = {}
    for k in clusters:
        new_clusters[k] = [] 
    logging.info("Start iteration ")
    for post in sim_dict.keys():
        best_cluster = get_best_cluster(post, clusters, sim_dict)    
        new_clusters[best_cluster].append(post)

    logging.info("Done iteration ")

    return new_clusters 

def choose_center(cluster, sim_dict):
    tot_sims = []
    for i in range(0, len(cluster)):
        post1 = cluster[i]
        tot_sim = 0
        for j in range(0, len(cluster)):
            if i == j:
                continue
            post2 = cluster[j]
            tot_sim += sim_dict[post1][post2]
        tot_sims.append((post1, tot_sim))

    center = sorted(tot_sims, key=lambda x: x[1])[0][0]

    return center

def choose_centers(clusters, sim_dict):
    logging.info("Start choose centers ")
    new_clusters = {}
    for cluster in clusters.keys():
        center = choose_center(clusters[cluster] + [cluster], sim_dict)
        if center != cluster:
            new_clusters[center] = map(lambda x: x, clusters[cluster])
            #new_clusters[center].remove(center)
            new_clusters[center].append(cluster)
        else:
            new_clusters[cluster] = clusters[cluster]

    logging.info("Done choose centers")

    return new_clusters

def get_cluster_dists(clusters, sim_dict):
    dists = {} 
    for c in clusters:
        cluster = clusters[c]
        ds = []
        for i in range(0, len(cluster)):
            for j in range (i + 1, len(cluster)):
               ds.append(sim_dict[cluster[i]][cluster[j]])

        if len(ds) > 1:
            dists[c] = reduce(lambda x, y: x+y, ds) / len(ds)
        elif len(ds) == 1:
            dists[c] = ds[0]
        else:
            dists[c] = 1

    return dists 

def init_clusters(sim_dict, clusters_num):
    init_clusters = sim_dict.keys()
    random.shuffle(init_clusters)

    return init_clusters[:clusters_num]
    #return init_clusters

def build_clusters_from_init(sim_dict, init_clusters):
    clusters = {}
    for p1 in sim_dict:
       sim_dict[p1][p1] = 0

    for c in init_clusters:
       clusters[c] = []
 
    for i in range(0,100):
        new_clusters = iteration(clusters, sim_dict)
        new_clusters = choose_centers(new_clusters, sim_dict)

        if new_clusters == clusters:
            break
        clusters = new_clusters

        #while True: 
        #    empty_cluster = None
        #    for c in clusters.keys():
        #        if len(clusters[c]) == 1:
        #            empty_cluster = c
        #            break
        #    if empty_cluster is not None:
        #        del clusters[empty_cluster]
        #        best_cluster = get_best_cluster(empty_cluster, clusters, sim_dict) 
        #        clusters[best_cluster].append(empty_cluster)
        #    else:
        #        break
  
    return new_clusters

def get_extra_cluster_dist(cl, sim_dict):
    cnt = 0.0
    dist = 0.0
    cl_arrays = map(lambda x: cl[x], cl.keys())
    for i in range(0, len(cl_arrays)):
        for j in range(i+1, len(cl_arrays)):
            for c1 in cl_arrays[i]:
                for c2 in cl_arrays[j]:
                    cnt += 1
                    dist += sim_dict[c1][c2]

    return dist / cnt

def get_intra_cluster_dist(cl, sim_dict):
    cnt = 0.0
    dist = 0.0

    for c_i in cl:
        c = cl[c_i]
        for i in range(0, len(c)):
            for j in range(i+1, len(c)):
                cnt += 1
                dist += sim_dict[c[i]][c[j]]

    return dist / cnt

def get_cluster_md5(cl):
    s = ",".join(map(str,cl)) 

    return util.digest_large(s)

#{ "clusters": 
#  { "members": 
#    {"id": , "text": ,"members_md5":}
#  }
#}
def get_clusters(sim_dict, clusters_num, nouns):
    cl = build_clusters(sim_dict, clusters_num)

    dists = get_cluster_dists(cl, sim_dict)

    logging.info("Get extra cluster avg dist")
    extra_dist = get_extra_cluster_dist(cl, sim_dict)
    logging.info("Extra dist: %s" % extra_dist)

    logging.info("Get intra cluster avg dist")
    intra_dist = get_intra_cluster_dist(cl, sim_dict)
    logging.info("Intra dist: %s" % intra_dist)
    logging.info("Intra/Extra dist: %s" % (intra_dist/extra_dist))

    cl2 = []
    avg_dist = 0.0
    for c in cl:
        struct = {  
            'members': map(lambda x: {'id': x, 'text': nouns[int(x)]}, cl[c]), 
            'members_len': len(cl[c]),
            'members_md5': get_cluster_md5(cl[c]),
            'avg_dist': "%.2f" % dists[c]
        }
        avg_dist += dists[c]
        cl2.append(struct)


    return {"clusters": cl2, "extra_dist": extra_dist, "intra_dist": intra_dist}

def build_clusters(sim_dict, clusters_num):
    cl = init_clusters(sim_dict, clusters_num) 

    return build_clusters_from_init(sim_dict, cl)
   
