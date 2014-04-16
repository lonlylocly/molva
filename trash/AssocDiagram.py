#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys,codecs
import time
import json
import os

import stats
from compiler.ast import flatten

import NounProfileStat

sys.stdout = codecs.getwriter('utf8')(sys.stdout)

db = 'replys_sharper.db'

class Hop:
    def __init__(self, path=None, post=None, hop=None):
        self.path = path
        self.post = post
        self.next_hop = hop

    def get_path(self):
        if self.next_hop is None:
            return self.path 
        else:
            return self.path * self.next_hop.get_path()

    def __str__(self):
        tail = ""
        if self.next_hop is not None:
            tail = self.next_hop.__str__()
        return "%s (%s); "  % (self.post, self.path)+ tail

    def verb(self, nouns):
        if self.post is None:
            return unicode(self.path)
        tail = u""
        if self.next_hop is not None:
            tail = self.next_hop.verb(nouns)
        return u"%s (%s); "  % (nouns[self.post], self.path)+ tail

def get_best_path(root, p, profiles_dict, previous_path, epsilon = 0.0001):
    if previous_path < epsilon:
        return [] 
    
    hops = []

    if p not in profiles_dict:
        return []

    replys_rel = profiles_dict[p].replys_rel
    for r in replys_rel:
        r_path = replys_rel[r] * previous_path
        if  r_path > epsilon:
            hops.append([r, r_path]) 

    for hop in hops:
        r = hop[0]
        r_path = hop[1]
        if r == root:
            continue
        hops += get_best_path(root, r, profiles_dict, r_path)

    hops_filtered = []
    hops_map = {}
    for h in hops:
        r = h[0]
        r_path = h[1]
        if r not in hops_map:
            hops_map[r] = r_path
        elif hops_map[r] < r_path:
            hops_map[r] = r_path
    
    for h in hops_map:
        if not (hops_map[h] >= 0.0 and hops_map[h] < 1.00001):
            print "%s %s" % (h, hops_map[h]) 
        assert hops_map[h] >= 0.0 and hops_map[h] < 1.00001
        hops_filtered.append([h, hops_map[h]])

    return hops_filtered

def get_best_path_for_nodes(p1, p2, profiles_dict, previous_path):

    if previous_path < 0.0001:
        return Hop(0)
    if p1 == p2:
        return Hop(1)

    assert p1 in profiles_dict
    assert p2 in profiles_dict

    best = Hop()

    replys_rel = profiles_dict[p1].replys_rel
    for r in set(replys_rel.keys()) & set(profiles_dict.keys()):
        r_path = replys_rel[r]
        if best.path is not None and r_path < best.get_path(): 
            continue
        
        r_hop = Hop(r_path, r, get_best_path(r, p2, profiles_dict, previous_path * r_path))

        if best.path is None or best.get_path() < r_hop.get_path():
            best = r_hop 


    return best 
        

def main():
    #input_file = sys.argv[1]
    output_file = sys.argv[1]


    cur = stats.get_cursor(db)
    nouns = stats.get_nouns(cur)
    tweets_nouns = stats.get_tweets_nouns(cur)
    profiles_dict = NounProfileStat.setup_noun_profiles(cur, tweets_nouns) 

    posts = profiles_dict.keys()

    f = codecs.open(output_file, "w", encoding="utf-8")
    chains = []
    for i in range(0, 1):
        p1 = posts[i]
        f.write(u"%s\n" % (nouns[p1]))
        print profiles_dict[p1].replys_rel
        chains = get_best_path(p1, p1, profiles_dict, 1) 

    for c in sorted(chains, key=lambda x: x[1], reverse=True):
        f.write(u"%s %s\n" % (nouns[c[0]], c[1]))

    f.close()


if __name__ == '__main__':
    main()
