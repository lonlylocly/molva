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
from util import digest

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

def get_nouns_replys(cur):
    r = cur.execute("""
        select post_noun_md5, reply_noun_md5 
        from noun_relations
    """).fetchall()

    nouns_replys = {}
    replys_nouns = {}
    for i in r:
        post = i[0]
        reply = i[1]
        if post in nouns_replys:
            nouns_replys[post].append(reply)
        else:
            nouns_replys[post] = [reply]
        if reply in replys_nouns:
            replys_nouns[reply].append(post)
        else:
            replys_nouns[reply] = [post]

    return (nouns_replys, replys_nouns)



def get_nouns(cur):
    res = cur.execute("""
        select noun_md5, noun from nouns
    """ ).fetchall()
    
    nouns = {}
    for r in res:
        nouns[r[0]] = r[1]
   
    return nouns

def get_tweets_nouns(cur):
    res = cur.execute("""
        select id, noun_md5 
        from tweets_nouns
    """).fetchall()
   
    ns_tw = {}
    for r in res:
        t_id, n = r
        n = int (n)
        t_id = int(t_id)
        if n not in ns_tw:
            ns_tw[n] = [t_id]
        else:
            ns_tw[n].append(t_id) 
    return ns_tw

def get_common_tweets(n1, n2, ns_tw):
    return set(ns_tw[n1]) & set(ns_tw[n2])

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

def klusterize_rand(cur, dists_file):
    #klusters = get_k_top_nouns(cur, k)
    dist = simdict.get_dists(dists_file)  
    #dist = simdict.get_dists('test.dump')  
    nouns_m = get_nouns(cur)
    nouns = nouns_m.keys()
    
    kluster_nouns = []
    klusters = []
    
    print "[%s] Start klusterize_rand" % (time.ctime())

    iter_cnt = 0

    while len(nouns) > 0:
        print "[%s] Iteration %s" % (time.ctime(), iter_cnt)
        iter_cnt = iter_cnt + 1
        next_noun, next_kluster, trash_k = klusterize_0(nouns, dist)
        nouns = trash_k
        kluster_nouns.append(next_noun)
        klusters.append(next_kluster)

    print "[%s] Done klusterize_rand" % (time.ctime())

    for i in range(0, len(klusters)):
        print "%s %s" % (kluster_nouns[i], len(klusters[i]))

    return [kluster_nouns, klusters]

def klusterize_0(nouns, dist):
    if len(nouns) == 0:
        return None 
    next_noun = get_k_random_nouns(1, nouns)[0]
    next_kluster = []
    trash_k = []
    
    print "[%s] Nouns len: %s " % (time.ctime(), len(nouns))

    for noun in nouns:
        dist_n = simdict.get_dist(noun, next_noun, dist)
        if dist_n < 1.0 and dist_n >= 0.0:
            next_kluster.append(noun)
        else:
            trash_k.append(noun)
    
    print "[%s] Kluster done. K: %s. Len(K): %s. Len(Trash): %s" % (time.ctime(), next_noun,
    len(next_kluster), len(trash_k))

    return [next_noun, next_kluster, trash_k]

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

def main_k():
    print "[%s] Startup" % time.ctime() 

    dists_file = '_noun_sim_reduced.dump'
    con = sqlite3.connect(db)
    con.isolation_level = None
    
    cur = con.cursor()
    k_map = klusterize_rand(cur, dists_file)
    f = open('klusters.json', 'w')     
    f.write(json.dumps(k_map, indent=4))
    f.close()
    print "[%s] End " % time.ctime() 

def elect_leaders(kls, dist):
    print "[%s] Reelect" % (time.ctime())
    kls2 = {}
    cnt = 0
    kls_keys_len = len(kls.keys())
    for k in kls.keys():
        print "[%s] Klusters %s of %s" % (time.ctime(), cnt, kls_keys_len)
        cnt += 1
        max_tot_dist = len(kls[k])
        print "[%s] Kluster power: %s " % (time.ctime(),max_tot_dist )
        leader = k
      
        for n1_i in range(0, len(kls[k]) - 1):
            n1 = kls[k][n1_i]
            tot_dist = 0
            for n2_i in range(n1_i + 1, len(kls[k])):
                n2 = kls[k][n2_i]
                tot_dist += simdict.get_dist(n1, n2, dist)            
            if tot_dist < max_tot_dist:
                max_tot_dist = tot_dist
                leader = n1
        kls2[leader] = kls[k]
    return kls2
        
def main_i(kl_file):
    print "[%s] Startup" % time.ctime() 
    #dist = simdict.get_dists('_noun_sim_reduced.dump')  
    kl = json.loads(open(kl_file, 'r').read())

    kls = {}
    for ind in range(0, len(kl[0])):
        k = kl[0][ind]
        val = kl[1][ind]
        if len(val) > 1:
            kls[k] = val
    
    #kls = elect_leaders(kls, dist)
    f = open('klusters-noleader.json','w')
    f.write(json.dumps(kls, indent=4))
    print "[%s] Done" % time.ctime() 

def get_common_replys(n1, n2, nouns_replys, nouns):
    n1_r = nouns_replys[n1]
    n2_r = nouns_replys[n2]
    n12_r = set(n1_r) & set(n2_r)
    
    n12_r = list(n12_r)
    n12_r = map(lambda x: nouns[x] if x in nouns else str(x), n12_r)

    return n12_r

if __name__ == "__main__":
    cmd = sys.argv[1]
    if cmd == 'kluster0':
        main_k()
    elif cmd == 'reelect':
        main_i(sys.argv[2])
