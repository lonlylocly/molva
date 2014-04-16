#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys,codecs
import time
import json
import os

import stats
from compiler.ast import flatten

db = 'replys_sharper.db'

#
# hierarchical clustering
# http://www.machinelearning.ru/wiki/images/2/28/Voron-ML-Clustering-slides.pdf
#
class LansCluster:
    def __init__(self, sim_dict):
        items = sim_dict.keys()
        self.clusters = map(lambda x: [x], items)
        self.sim_tree = []
        self.sim_dict = sim_dict
        for i in items:
            self.sim_tree.append([])
            for j in items:
                if i == j:
                    sim_dict[i][j] = 0
                self.sim_tree[-1].append(sim_dict[i][j])

    def build(self):
        while True:
            print "[%s] start iteration" %  (time.ctime())
            i, j, dist = self.get_closest()
            
            if i is None or j is None:
                return

            print "[%s] Got closest (%d, %d, %f)" % (time.ctime(), i, j, dist)
            self.unite(i, j)

    def unite(self, i, j):

        print "[%s] unite %d, %d" %  (time.ctime(), i, j)
        assert i >= 0 and i < len(self.clusters) 
        assert j >= 0 and j < len(self.clusters) 
        
        new_cl = (self.clusters[i], self.clusters[j])
        self.clusters.append(new_cl)
            
        new_k = len(self.clusters) - 1 
        self.sim_tree.append(map( lambda x: [], self.clusters))

        print [i, j, new_k]
        for k in xrange(0, new_k):
            if self.clusters[k] is None:
                continue
            self.sim_tree[k].append([])
            self.sim_tree[k][new_k] = self.get_dist(k, i, j)
            self.sim_tree[new_k][k] = self.sim_tree[k][new_k]

        self.sim_tree[new_k][new_k] = 0
        assert len(self.sim_tree[new_k]) == len(self.clusters)
        #assert len(self.sim_tree[new_k - 2]) == len(self.clusters)
        #assert len(self.sim_tree[new_k - 1]) == len(self.clusters)

        #print self.sim_tree[new_k-1][new_k]

        self.clusters[i] = None
        self.clusters[j] = None

    def get_dist(self, k, i, j):
        k_l = flatten(self.clusters[k])
        w_l = flatten(self.clusters[i]) + flatten(self.clusters[j])

        return LansCluster.get_group_dist(k_l, w_l, self.sim_dict)        

    @staticmethod
    def get_group_dist(cl1, cl2, sim_dict):
        tot_sum = 0
        for cl1_i in cl1:
            for cl2_i in cl2:
                tot_sum += sim_dict[cl1_i][cl2_i]
        tot_len = (len(cl1) * len(cl2))
        if tot_len != 0:
            tot_sum = tot_sum / tot_len 

        return tot_sum

    def get_closest(self):
        min_dist = None 
        best_i = None
        best_j = None
        for i in xrange(0, len(self.clusters)):
            if self.clusters[i] is None:
                continue
            for j in xrange(0, len(self.clusters)):
                try:
                    if self.clusters[j] is None:
                        continue
                    if i == j:
                        continue
                    if min_dist is None or self.sim_tree[i][j] < min_dist:
                        min_dist = self.sim_tree[i][j]
                        best_i = i
                        best_j = j
                except Exception as e:
                    print len(self.sim_tree[i])
                    print len(self.clusters)
                    print [i, j]
                    raise e
                    
        return (best_i, best_j, min_dist)

def main():
    input_file = sys.argv[1]
    output_file = sys.argv[2]

    sim_dict = json.load(open(input_file, "r"))
    for p1 in sim_dict:
        sim_dict[p1][p1] = 0

    cur = stats.get_cursor(db)
    nouns = stats.get_nouns(cur)

    l = LansCluster(sim_dict)

    l.build()

    print json.dumps(l.clusters, indent=4)

    f = open(output_file, "w")

    f.write(json.dumps(l.clusters, indent=4))

    f.close()

if __name__ == '__main__':
    main()
