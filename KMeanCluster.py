#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys,codecs
import time
import json
import os
import random

import stats

sys.stdout = codecs.getwriter('utf8')(sys.stdout)

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

def get_groups_matrix(clusters, nouns, dists, k=4):
    groups = []
    for i in clusters:
        group = [str(dists[i])]
        group += map(lambda x: nouns[int(x)], clusters[i])
        groups.append(group)

    return _get_groups_matrix(groups, nouns, k)

def _get_groups_matrix(groups, nouns, k=4):
    groups = sorted(groups, key=lambda x: float(x[0]))
    groups2 = []
    for i in range(0, len(groups) / k + 1):
        groups2.append([])
        for j in range(0, k):
            if (i * k + j) >= len(groups):
                break
            groups2[-1].append("\n".join(groups[i*k + j]))
    
    return groups2

def uncode_nouns(nouns_list, nouns):
    return map(lambda x: nouns[int(x)], nouns_list)

def get_merged_groups_matrix(cluster1, cluster2, nouns, k=4):
    groups = []
    for cl in set(cluster1.keys()) | set(cluster2.keys()):
        common = set(cluster1[cl]) & set(cluster2[cl]) if cl in cluster1 and cl in cluster2 else set()
        first = set(cluster1[cl]) - common if cl in cluster1 else set()
        second = set(cluster2[cl]) - common if cl in cluster2 else set()
        groups.append([])

        common = uncode_nouns(common, nouns)
        first = uncode_nouns(first, nouns)
        second = uncode_nouns(second, nouns)

        groups[-1] += list(common)
        groups[-1] += list(map(lambda x: "<span style='color: red'>" + x + "</span>", first ))
        groups[-1] += list(map(lambda x: "<span style='color: blue'>" + x + "</span>", second ))

    return _get_groups_matrix(groups, nouns, k)

def write_groups_matrix(groups, output_file):
    fout = codecs.open(output_file, 'w', encoding='utf-8')
    fout.write("<html><head><meta charset=\"UTF-8\"></head><body><table border=\"1\">")
    for i in groups:
        fout.write("<tr>\n")
        for j in i:
            fout.write("<td><pre>" + j + "</pre></td>")    
        fout.write("<tr>\n")
    fout.write("</table></body></html>")
    fout.close()

def average_similarity(sim_dict, post):
    avg = 0
    for k in sim_dict[post]:
        avg += sim_dict[post][k]

    avg = avg / len(sim_dict[post])

    return avg

def good_similarity(sim_dict, post, goodness=0.4):
    avg = 0
    for k in sim_dict[post]:
        if sim_dict[post][k] < goodness:
            avg += 1

    avg = float(avg) / len(sim_dict[post])

    return 1.0 - avg

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
 
    while True:
        new_clusters = iteration(clusters, sim_dict)
        new_clusters = choose_centers(new_clusters, sim_dict)

        if new_clusters == clusters:
            break
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
  
    return new_clusters

def get_clusters(sim_dict, clusters_num, nouns):
    cl = build_clusters(sim_dict, clusters_num)

    dists = get_cluster_dists(cl, sim_dict)

    cl2 = []
    for c in cl:
        struct = {  
            'members': map(lambda x: {'id': x, 'text': nouns[int(x)]}, cl[c]), 
            'members_len': len(cl[c]),
            'avg_dist': "%.2f" % dists[c]
        }
        cl2.append(struct)

    return cl2

def build_clusters(sim_dict, clusters_num):
    cl = init_clusters(sim_dict, clusters_num) 

    return build_clusters_from_init(sim_dict, cl)
   
def main(input_file, clusters_num):
    sim_dict = json.load(open(input_file, "r"))
    return build_clusters(sim_dict, clusters_num)

if __name__ == '__main__':
    input_file = sys.argv[1]
    clusters_num = int(sys.argv[2])
    output_file = sys.argv[3]

    sim_dict = json.load(open(input_file, "r"))
    cl1 = build_clusters(sim_dict, clusters_num)
    #cl2 = main(input_file, clusters_num, output_file)

    db = os.environ["MOLVA_DB"]

    cur = stats.get_cursor(db)
    nouns = stats.get_nouns(cur)
 
    dists = get_cluster_dists(cl1, sim_dict)

    groups = get_groups_matrix(cl1, nouns, dists, k=8)
    write_groups_matrix(groups, output_file)

    json.dump(cl1, open("clusters.json", "w"))


