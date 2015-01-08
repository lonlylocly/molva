#!/usr/bin/python
# -*- coding: utf-8 -*-
import logging, logging.config
import json
import numpy

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

    #print "\t".join(map(lambda x: "%10s" % x,weights))
    #print "\t".join(map(lambda x: "%10s" % x,edges))

def count(start, end):
    logging.info("load %s" % start)
    p1 = json.load(open(start,'r'))
    logging.info("load %s" % end)
    p2 = json.load(open(end,'r'))

    logging.info("Start comparing")
    measures = []
    for k in set(p1.keys()) | set(p2.keys()):
        if k not in p1 or k not in p2:
            measures.append((k, None))
            continue
        pr1 = get_profile(p1[k], k)
        pr2 = get_profile(p2[k], k)
        measures.append((k, ProfileCompare(pr1, pr2).dist))

    misses = 0
    vals = []
    for m in measures:
        if m[1] is None:
            misses += 1
        else:
            vals.append(m[1])
    
    return {
        "d1": start,
        "d2": end,
        "total": len(measures),
        "mean": numpy.mean(vals),
        "stddev": numpy.std(vals)
    }

def main():
    parser = util.get_dates_range_parser()
    args = parser.parse_args()

    res = count(args.start, args.end)

    print "Self check: equal %s" % ProfileCompare(get_profile({"1": 1}, 1), get_profile({"1": 1},1)).dist 
    print "Self check: not equal %s" % ProfileCompare(get_profile({"1": 1}, 1), get_profile({"2": 1},1)).dist

    #print "total keys %s" % len(measures)
    
    print "\t" + "\t".join(["d1","d2", "total keys",  "avg cosine, mean", "avg cosine, std dev"])
    print "\t" + "\t".join(map(str,[res["d1"], res["d2"], res["total"], res["mean"], res["stddev"]]))
    
    #print "missed keys %s"  % misses 
    #print "mean %s" % numpy.mean(vals)
    #print "std dev %s" % numpy.std(vals)
    #print_histogram(numpy.histogram(vals, range=(0,1.0),density=True))

if __name__ == '__main__':
    main()
