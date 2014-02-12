#!/usr/bin/python
# -*- coding: utf-8 -*-
import sqlite3
from subprocess import Popen, PIPE, STDOUT
import sys,codecs
import re
import hashlib
import time
import json
import random
import simdict

sys.stdout = codecs.getwriter('utf8')(sys.stdout)

db = 'more_replys2.db'

# союзы всякие несчастные
blocklist = [127460222,194911016,348664285,522559933,1543116322,1928061337,2860766309,3235057763,3534832033,3759046491]
bl = ",".join(map(lambda x: str(x), blocklist))

MAX_DIST = 1

def get_k_top_nouns(cur, k):
    res = cur.execute("""
        select noun_md5, count(*) as freq
        from nouns
        where noun_md5 not in (%s)
        group by noun_md5
        order by freq desc
        limit %s
    """ % (bl,k)).fetchall()
    
    return map(lambda x: x[0], res)


def get_nouns(cur):
    res = cur.execute("""
        select noun_md5, noun from nouns
        where noun_md5 not in (%s) 
    """ % bl).fetchall()
    
    nouns = {}
    for r in res:
        nouns[r[0]] = r[1]
   
    for b in blocklist:  
        assert b not in nouns

    return nouns

def pick_best_kluster(noun, klusters, dist):
    best_k = 0
    best_dist = MAX_DIST 
    for k_i in range(len(klusters)):
        k_noun = klusters[k_i]
        dist_n_k = simdict.get_dist(noun, k_noun, dist)
        if dist_n_k < best_dist:
            best_k = k_i
            best_dist = dist_n_k

    return best_k

def elect_leader(k_map, dist, klusters):
    leaders = []
    #print "klusters: %s" % str(klusters)
    for k_i in range(len(k_map)):
        print "[%s] for kluster %s (%s)" % (time.ctime(), k_i, klusters[k_i])
        k_list = k_map[k_i]
        if len(k_list) == 0:
            leaders.append(klusters[k_i])
            continue
        best_leader = 0
        best_sum_dist = len(k_list) * MAX_DIST
        #print "start: best sum dist: %s" % best_sum_dist 
        for i in range(len(k_list)):        
            sum_dist = 0
            n = k_list[i]
            for n2 in k_list:
                sum_dist += simdict.get_dist(n,n2,dist)    
            if sum_dist < best_sum_dist:
                best_leader = i
                best_sum_dist = sum_dist
                #print "change: sum dist: %s" % (sum_dist)

        #print "final: best sum dist: %s" % best_sum_dist 
        leaders.append(k_list[best_leader])

    return leaders


def get_k_random_nouns(k, nouns):
    random.seed(time.time())
    rlist = []
    while len(rlist) < k:
        next_i = int((random.random() * len(nouns)))
        #print "next i: %s" % next_i
        n = nouns[next_i]
        if n not in rlist:
            rlist.append(n)
    return rlist

def klusterize(cur, k):
    #klusters = get_k_top_nouns(cur, k)
    dist = simdict.get_dists('_noun_sim_reduced.dump')  
    #dist = simdict.get_dists('test.dump')  
    nouns_m = get_nouns(cur)
    nouns = nouns_m.keys()
    
    klusters = get_k_random_nouns(k, nouns)
    print klusters

    k_map_prev = []

    for i in range(100):
        print "[%s] Start iteration %s " % (time.ctime(), i)
        k_map = map(lambda x: [], klusters)

        for noun in nouns:
            best_k = pick_best_kluster(noun, klusters, dist) 
            k_map[best_k].append(noun)

        if k_map == k_map_prev:
            print "Klusters found"
            break
        k_map_prev = k_map
        
        print "[%s] reelect leader " % time.ctime()
        klusters = elect_leader(k_map, dist, klusters)
    
    k_map_fin = {}
    for i in range(len(klusters)):
        if len(k_map_prev[i]) == 0:
            continue
        k_noun = nouns_m[klusters[i]]
        k_map_fin[k_noun] = {
        'i': i,
        'len': len(k_map_prev[i]),
        'some10': map(lambda x: nouns_m[x], k_map_prev[i][0:10])}

    return k_map_fin

def main():
    print "[%s] Startup" % time.ctime() 

    con = sqlite3.connect(db)
    con.isolation_level = None
    
    cur = con.cursor()
    k_map = klusterize(cur, 10)
    f = open('klusters.json', 'w')     
    f.write(json.dumps(k_map, indent=4))
    f.close()
    print "[%s] End " % time.ctime() 

if __name__ == "__main__":
    main()
