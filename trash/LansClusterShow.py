#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys,codecs
import time
import json
import os

import math

import stats
from compiler.ast import flatten

from LansCluster import LansCluster

sys.stdout = codecs.getwriter('utf8')(sys.stdout)


db = 'replys_sharper.db'

cur = stats.get_cursor(db)
nouns = stats.get_nouns(cur)


def to_tree(clusters, parent, tree):
    if len(clusters) == 1:
        print clusters
        tree.append({"parent": parent, "id": clusters[0], "text": nouns[int(clusters[0])]})
    else:
        assert len(clusters) == 2
        
        parent2 = parent + 1
        tree.append({"parent": parent, "id": parent2, "text": "-"})

        to_tree(clusters[0], parent2, tree)
        to_tree(clusters[1], parent2, tree)

def to_tree2(clusters, parent):
    if len(clusters) == 1:
        return {"text": nouns[int(clusters[0])]}
    else:
        return {"text": parent +1, "children": [
        to_tree2(clusters[0], parent + 1),
        to_tree2(clusters[1], parent + 1)
        ]}

def to_dot(clusters, parent, node_iter, dots, nouns):
    if len(clusters) == 1:
        dots.append(u"%s [label=\"%s\"];" % (clusters[0], nouns[int(clusters[0])]))
        dots.append(u"%s -- %s;" % (parent, clusters[0]))
    else:
        parent2 = node_iter.next()
        dots.append(u"%s -- %s;" % (parent, parent2)) 
        to_dot(clusters[0], parent2, node_iter, dots, nouns),
        to_dot(clusters[1], parent2, node_iter, dots, nouns)

def get_k_groups(k, clusters):
    if k <= 1:
        return [clusters]
    if len(clusters) == 1:
        return [clusters]
    else:
        c1 = len(flatten(clusters[0]))
        c2 = len(flatten(clusters[1]))
        if c1 == 1:
            return get_k_groups(k, clusters[1])
        if c2 == 1:
            return get_k_groups(k, clusters[0])

        k_port =  float(c1) / (c1 +c2)
        k1 = int(math.ceil(k_port * (k )))
        k2 = k  - k1
        #print "K %s, c1 %s, c2 %s, k_port %s, k1 %s, k2 %s" % (k, c1, c2, k_port, k1, k2) 
        g1 = get_k_groups(k1, clusters[0])
        g2 = get_k_groups(k2, clusters[1])
        return g1 + g2


def main():
    input_file = sys.argv[1]
    simdict_file = sys.argv[2]
    output_file = sys.argv[3]


    clusters = json.load(open(input_file, "r"))
    clusters = filter(lambda x: x is not None, clusters)
    clusters = clusters[0]

    sim_dict = json.load(open(simdict_file, "r"))
    for p1 in sim_dict:
        sim_dict[p1][p1] = 0

    gr = get_k_groups(int(sys.argv[3]), clusters)

    #print len(gr)

    gr2 = []
    for g in gr:
        g_f = flatten(g)
        dist = LansCluster.get_group_dist([g_f[0]], g_f[1:], sim_dict)
        group = u"\t".join(map(lambda x: nouns[int(x)], g_f))
        if dist != 0:
            gr2.append([group, dist])

    for gr in sorted(gr2, key=lambda x: x[1]):
        print ""
        print "%f - %s" % (gr[1], gr[0]) 
    
if __name__ == '__main__':
    main()
