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

sys.stdout = codecs.getwriter('utf8')(sys.stdout)

db = 'replys.db'

# союзы всякие несчастные
blocklist = [127460222,194911016,348664285,522559933,1543116322,1928061337,2860766309,3235057763,3534832033,3759046491]
bl = ",".join(map(lambda x: str(x), blocklist))

MAX_DIST = 1

def get_distances(cur):
    res = cur.execute("""
        select n1_md5, n2_md5, 1 / (similar * (p.sum_count + p2.sum_count)) as sim_k 
        from ns_view s   
        inner join post_sum_count p 
        on s.n1_md5 = p.noun_md5  
        inner join post_sum_count p2 
        on s.n2_md5 = p2.noun_md5
        where 
        s.n1_md5 < s.n2_md5
        and s.n1_md5 not in (%s)  
        and s.n2_md5 not in (%s)
        and similar > 0 
        order by sim_k asc  
    """ % (bl, bl)).fetchall()
    min_dist = 1000
    max_dist = 0    
    dists = {} 
    for r in res:
        n1, n2, dist = r
        if dist > max_dist:
            max_dist = dist
        if dist < min_dist:
            min_dist = dist

        if n1 not in dists:
            dists[n1] = {}
        if n2 not in dists:
            dists[n2] = {}
        dists[n1][n2] = dist        
        dists[n2][n1] = dist        
        dists[n1][n1] = 0
        dists[n2][n2] = 0

    print "max dist: %s; min dist: %s" % (max_dist, min_dist)

    for b in blocklist:  
        assert b not in dists

    return dists

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
    print bl
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

def get_dist(n1, n2, dist):
    if n1 in dist:
        try:
            if n2 in dist[n1]:
                return dist[n1][n2]
        except Exception as e:
            print e
            print dist[n1]
    return MAX_DIST 

def pick_best_kluster(noun, klusters, dist):
    best_k = 0
    best_dist = MAX_DIST 
    for k_i in range(len(klusters)):
        k_noun = klusters[k_i]
        dist_n_k = get_dist(noun, k_noun, dist)
        if dist_n_k < best_dist:
            best_k = k_i
            best_dist = dist_n_k

    return best_k

def elect_leader(k_map, dist, klusters):
    leaders = []
    #print "klusters: %s" % str(klusters)
    for k_i in range(len(k_map)):
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
                sum_dist += get_dist(n,n2,dist)    
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
    dist = get_distances(cur)
    nouns_m = get_nouns(cur)
    nouns = nouns_m.keys()
    
    klusters = get_k_random_nouns(k, nouns)

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
        k_map_fin[k_noun] = {'len': len(k_map_prev[i]),
        'top10': map(lambda x: nouns_m[x], k_map_prev[i][0:10])}

    return k_map_fin

def main():
    print "[%s] Startup" % time.ctime() 

    con = sqlite3.connect(db)
    con.isolation_level = None
    
    cur = con.cursor()
    k_map = klusterize(cur, 100)
    f = open('klusters.json', 'w')     
    f.write(json.dumps(k_map, indent=4))
    f.close()
    print "[%s] End " % time.ctime() 

if __name__ == "__main__":
    main()
