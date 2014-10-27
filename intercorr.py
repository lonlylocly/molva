#!/usr/bin/python
# -*- coding: utf-8 -*-
import logging, logging.config
import json
import numpy
import math

import util
from profile import NounProfile, ProfileCompare

logging.config.fileConfig("logging.conf")

def get_profile(profile, k):
    pr = NounProfile(k)
    pr.replys = profile
    return pr

def print_histogram(arr):
    weights = map(str,arr[0])
    edges = map(str,arr[1])

    print "\t".join(weights)
    print "\t".join(edges)

def get_dict(f):
    d={}
    logging.info("load %s" % f)
    p1 = open(f,'r')

    logging.info("Start filling")
    measures = []
    while True:
        l = p1.readline()
        if l is None or l == '':
            break
        k1, k2, val = l.split(';') 
        k1 = int(k1)
        k2 = int(k2)
        val = float(val.replace(",",'.'))
        if k1 > k2:
            k1, k2 = (k2, k1)

        if k1 not in d:
            d[k1] = {}
        d[k1][k2] = val
    
    return d 

def get_measures(d, common_keys):
    measures = []
    for k1 in sorted(d.keys()):
        if k1 not in common_keys:
            continue
        for k2 in sorted(d[k1].keys()):
            if k2 not in common_keys:
                continue
            measures.append((k1, k2, d[k1][k2]))

    return measures 

def get_pearson(m1, m2):
    m1_mean = numpy.mean(m1)
    m2_mean = numpy.mean(m2)

    assert len(m1) == len(m2)

    sum_xy = 0
    sum_x2 = 0
    sum_y2 = 0
    for i in range(0, len(m1)):
        x = m1[i]
        y = m2[i]
        sum_xy += (x - m1_mean) * (y - m2_mean)
        sum_x2 +=   (x - m1_mean) ** 2
        sum_y2 +=   (y - m2_mean) ** 2

    #print "sum_xy: %s; sum_x2: %s; sum_y2: %s; m1_mean: %s; m2_mean: %s" % (sum_xy, sum_x2, sum_y2, m1_mean, m2_mean)
    r = sum_xy / (math.sqrt(sum_x2) * math.sqrt(sum_y2)) 
   
    return r 

class Ranked:
    def __init__(self, orig_ind, value ):
        self.orig_ind = orig_ind
        self.value = value

def get_ranked(m):
    m_new = []
    for orig_ind in range(0, len(m)):
        m_new.append(Ranked(orig_ind, m[orig_ind]))
    
    m_new = sorted(m_new, key=lambda x: x.value)
    ranks = map(lambda x: None, range(0, len(m)))
    for rank_ind in range(0, len(m)):
        ranks[m_new[rank_ind].orig_ind] = rank_ind

    return ranks

def get_spearman(m1, m2):
    m1 = get_ranked(m1)
    m2 = get_ranked(m2)

    ranks = 0
    for i in range(0, len(m1)):
        ranks += (m1[i] - m2[i]) ** 2 
    
    p = float(6 * ranks) / (len(m1) * (len(m1) ** 2 - 1))

    #print "ranks: %s; p: %s" % (ranks, p)

    r = 1 - p 

    return r

def get_keys(d):
    ks = set()
    for k in d:
        ks.add(k)
        for k2 in d[k]:
            ks.add(k2)

    return list(ks)

def count(start, end):
    d1 = get_dict(start)
    d2 = get_dict(end)
    
    d1_keys = get_keys(d1)
    d2_keys = get_keys(d2)
    
    common_keys = set(d1_keys) & set(d2_keys)
    #print "common keys len: %s" % len(common_keys)

    #print "len k1: %s; len k2: %s" % (len(d1_keys), len(d2_keys))
    
    logging.info("Start comparing")
    m1 = get_measures(d1, common_keys)
    m2 = get_measures(d2, common_keys)

    #print "len m1: %s; len m2: %s" % (len(m1), len(m2))

    assert len(m1) > 0
    assert len(m2) > 0

    for i in range(0, len(m1)):
        assert m1[i][0] == m2[i][0]
        assert m1[i][1] == m2[i][1]
 
    r_p = get_pearson(map(lambda x: x[2], m1), map(lambda x: x[2], m2))

    r_s = get_spearman(map(lambda x: x[2], m1), map(lambda x: x[2], m2))

    return {"pearson": r_p, "spearman": r_s}

def main():
    parser = util.get_dates_range_parser()
    args = parser.parse_args()

    res = count(args.start, args.end)

    print "\t" + "\t".join(["d1", "d2", "Pearson", "Spearman"])
    print "\t" + "\t".join([args.start, args.end, r_p, r_s])

    #print "m1\tm2"
    #for i in range(0,100):
    #    print "%s\t%s" % (m1[i][2], m2[i][2])

if __name__ == '__main__':
    main()
