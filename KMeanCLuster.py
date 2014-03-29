#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys,codecs
import time
import json
import os

import stats

sys.stdout = codecs.getwriter('utf8')(sys.stdout)

db = 'replys_sharper.db'

def get_best_cluster(post, clusters, sim_dict):
    cluster_sims = map( lambda x: (x, sim_dict[post][x]), clusters.keys())
    best_cluster = sorted(cluster_sims, key= lambda x: x[1])[0][0]

    return best_cluster

def iteration(clusters, sim_dict):
    new_clusters = {}
    for k in clusters:
        new_clusters[k] = [] 
    print "[%s] Start iteration " % (time.ctime())
    for post in sim_dict.keys():
        best_cluster = get_best_cluster(post, clusters, sim_dict)    
        new_clusters[best_cluster].append(post)

    print "[%s] Done iteration " % (time.ctime())

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
    print "[%s] Start choose centers " % (time.ctime())
    new_clusters = {}
    for cluster in clusters.keys():
        center = choose_center(clusters[cluster] + [cluster], sim_dict)
        if center != cluster:
            new_clusters[center] = map(lambda x: x, clusters[cluster])
            #new_clusters[center].remove(center)
            new_clusters[center].append(cluster)
        else:
            new_clusters[cluster] = clusters[cluster]

    print "[%s] Done choose centers " % (time.ctime())

    return new_clusters

def main():
    input_file = sys.argv[1]
    clusters_num = int(sys.argv[2])
    output_file = sys.argv[3]

    sim_dict = json.load(open(input_file, "r"))
    for p1 in sim_dict:
        sim_dict[p1][p1] = 0

    clusters = {}
    for c in sim_dict.keys()[0:clusters_num]:
        clusters[c] = []

    while True:
        new_clusters = iteration(clusters, sim_dict)
        new_clusters = choose_centers(new_clusters, sim_dict)

        #print new_clusters
        #print clusters
        if new_clusters == clusters:
            break
        clusters = new_clusters

    clusters = new_clusters

   
    while True: 
        empty_cluster = None
        for c in clusters.keys():
            if len(clusters[c]) == 1:
                empty_cluster = c
                break
        if empty_cluster is not None:
            del clusters[empty_cluster]
            best_cluster = get_best_cluster(empty_cluster, clusters, sim_dict) 
            clusters[best_cluster].append(empty_cluster)
        else:
            break
                        
                
   
    fout = open(output_file, 'w')
    json.dump(new_clusters, fout, indent=4) 
    fout.close()

    cur = stats.get_cursor('replys_sharper.db')
    nouns = stats.get_nouns(cur)

    for c in new_clusters:
        print "%s" % (nouns[int(c)])
        for l in new_clusters[c]:
            print "\t%s" % (nouns[int(l)])
if __name__ == '__main__':
    main()
