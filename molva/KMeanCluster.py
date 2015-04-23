#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys,codecs
import time
import json
import os
import random
import logging

import stats
import molva.util as util

sys.stdout = codecs.getwriter('utf8')(sys.stdout)

class KMeansClusteriser:

    def __init__(self, sim_dict, clusters_num, max_iter=100, trash_words=None):
        self.sim_dict = sim_dict
        self.clusters_num = clusters_num
        self.clusters = {}
        self.max_iter = max_iter
        self.trash_words = trash_words if trash_words is not None else []


    def build_clusters(self):
        cl = self.init_clusters() 

        return self.build_clusters_from_init(cl)

    def init_clusters(self):
        init_clusters = self.sim_dict.keys()
        random.shuffle(init_clusters)
        filtered_init_clusters = []
        filtered_cnt = 0
        for c in init_clusters:
            if c in self.trash_words:
                 filtered_cnt += 1
            else:
                filtered_init_clusters.append(c)

        logging.info("Filtered %s init clusters (trash words)" % filtered_cnt)

        return filtered_init_clusters[:self.clusters_num]

    def build_clusters_from_init(self, init_clusters):
        self.clusters = {}
        for p1 in self.sim_dict:
           self.sim_dict[p1][p1] = 0

        for c in init_clusters:
           self.clusters[c] = []
     
        for i in range(0,self.max_iter):
            new_clusters = self.iteration() 
            new_clusters = self.choose_centers(new_clusters) #

            if new_clusters == self.clusters:
                break
            self.clusters = new_clusters
      
        return self.clusters

    def iteration(self):
        new_clusters = {}
        for k in self.clusters:
            new_clusters[k] = [] 
        logging.info("Start iteration ")
        for post in self.sim_dict.keys():
            best_cluster = self.get_best_cluster(post, self.clusters.keys())    
            new_clusters[best_cluster].append(post)

        logging.info("Done iteration ")

        return new_clusters 

    def _get_cluster_sims(self, post, current_centroids):
        post_sims = self.sim_dict[post]
        return map( lambda x: (x, post_sims[x]), current_centroids)

    def _get_best_cluster(self, cluster_sims):
        best_pair = cluster_sims[0]
        for i in xrange(1, len(cluster_sims)):
            cur = cluster_sims[i]
            if cur[1] < best_pair[1]:
                best_pair = cur

        return best_pair[0]
    
    def get_best_cluster(self, post, current_centroids):
        cluster_sims = self._get_cluster_sims(post, current_centroids)
        best_cluster = self._get_best_cluster(cluster_sims) 

        return best_cluster

    def choose_centers(self, clusters2):
        logging.info("Start choose centers ")
        new_clusters = {}
        for cluster in clusters2.keys():
            center = self.choose_center(clusters2[cluster] + [cluster])
            if center != cluster:
                new_clusters[center] = map(lambda x: x, clusters2[cluster])
                new_clusters[center].append(cluster)
            else:
                new_clusters[cluster] = clusters2[cluster]

        logging.info("Done choose centers")

        return new_clusters


    def choose_center(self, cluster):
        tot_sims = []
        filtered_words = 0
        for i in range(0, len(cluster)):
            post1 = cluster[i]
            if post1 in self.trash_words:
                filtered_words += 1
                continue
            tot_sim = 0
            for j in range(0, len(cluster)):
                if i == j:
                    continue
                post2 = cluster[j]
                tot_sim += self.sim_dict[post1][post2]
            tot_sims.append((post1, tot_sim))

        center = sorted(tot_sims, key=lambda x: x[1])[0][0]

        if filtered_words > 0:
            logging.info("Excluded from cluster center %s words (trash words)" % filtered_words)
        
        return center


    def get_cluster_dists(self):
        dists = {} 
        for c in self.clusters:
            cluster = self.clusters[c]
            ds = []
            for i in range(0, len(cluster)):
                for j in range (i + 1, len(cluster)):
                   ds.append(self.sim_dict[cluster[i]][cluster[j]])

            if len(ds) > 1:
                dists[c] = reduce(lambda x, y: x+y, ds) / len(ds)
            elif len(ds) == 1:
                dists[c] = ds[0]
            else:
                dists[c] = 1

        return dists 

    def get_extra_cluster_dist(self):
        cnt = 0.0
        dist = 0.0
        cl_arrays = self.clusters.values() 
        for i in range(0, len(cl_arrays)):
            for j in range(i+1, len(cl_arrays)):
                for c1 in cl_arrays[i]:
                    for c2 in cl_arrays[j]:
                        cnt += 1
                        dist += self.sim_dict[c1][c2]

        return dist / cnt

    def get_intra_cluster_dist(self):
        cnt = 0.0
        dist = 0.0

        for c in self.clusters.values():
            for i in range(0, len(c)):
                for j in range(i+1, len(c)):
                    cnt += 1
                    dist += self.sim_dict[c[i]][c[j]]

        return dist / cnt

    def get_cluster_md5(self, cl):
        s = ",".join(map(str,cl)) 

        return util.digest_large(s)

#{ "clusters": 
#  { "members": 
#    {"id": , "text": ,"members_md5":}
#  }
#}
def get_clusters(sim_dict, clusters_num, nouns, trash_words=None):
    kmeans = KMeansClusteriser(sim_dict, clusters_num, trash_words=trash_words)
    cl = kmeans.build_clusters()

    dists = kmeans.get_cluster_dists()

    logging.info("Get extra cluster avg dist")
    extra_dist = kmeans.get_extra_cluster_dist()
    logging.info("Extra dist: %s" % extra_dist)

    logging.info("Get intra cluster avg dist")
    intra_dist = kmeans.get_intra_cluster_dist()
    logging.info("Intra dist: %s" % intra_dist)
    logging.info("Intra/Extra dist: %s" % (intra_dist/extra_dist))

    cl2 = []
    avg_dist = 0.0
    for c in cl:
        struct = {  
            'members': map(lambda x: {'id': x, 'text': nouns[int(x)]}, cl[c]), 
            'members_len': len(cl[c]),
            'members_md5': str(kmeans.get_cluster_md5(cl[c])),
            'avg_dist': "%.2f" % dists[c],
            'centroid_md5': str(c),
            'centroid_text': nouns[int(c)]
        }
        avg_dist += dists[c]
        cl2.append(struct)


    return {"clusters": cl2, "extra_dist": extra_dist, "intra_dist": intra_dist}

   
