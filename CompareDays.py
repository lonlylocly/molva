#!/usr/bin/python
import json
import sys
import os
import math

import stats

import KMeanCluster

def count_diff(d1, d2):
    return (d1 - d2) 

def compare_profiles(day1, day2):
    diff = []
    for post in day1:
        if post not in day2:
            #diff.append([float("infinity"), post])
            continue
        for post2 in day1[post]:
            if int(post) < int(post2):
                continue 
            if post2 not in day2[post]:
                #diff.append([float("infinity"), post, post2])
                continue
            diff.append([count_diff(day1[post][post2], day2[post][post2]), day1[post][post2], day2[post][post2], post, post2])
      
    #diff = filter(lambda x: math.fabs(x[0]) <= 0.0001, diff) 
    splays = lambda x: (1 / (1 + math.fabs(x[0])), x[1] + x[2])
    most_wow =lambda x: math.fabs(x[0]) 
    most_wow2 =lambda x: (min(x[1], x[2]), math.fabs(x[0]) )
    diff = sorted(diff, key=most_wow2, reverse=False)
 
    cur = stats.get_cursor(os.environ["MOLVA_DB"])
    nouns = stats.get_nouns(cur)

    for d in diff[:100]:
        #if d[0] > 0.001:
        #    break
        print "%s\t%s\t%s\t%s\t%s" % (d[0], d[1], d[2], nouns[int(d[3])], nouns[int(d[4])])


def com_coef(cl1, cl2):
    intersect = float(len(set(cl1) & set(cl2)))
    com = len(set(cl1) | set(cl2))

    return  intersect / com 

def compare_clusters(cl1, cl2, nouns):
    cl1 = cl1.values()
    cl2 = cl2.values()

    cmp_stat = []
    for c1 in cl1:
        best_match = 0
        best_c2 = []
        for c2 in cl2:
            coef = com_coef(c1, c2)
            if  coef > best_match:
                best_match = coef
                best_c2 = c2

        cmp_stat.append([c1, best_c2, best_match])

    for item in sorted(cmp_stat, key=lambda x: x[2], reverse=True):
        c1, c2, match = item
        print match
        for i in set(c1) | set(c2):
            if i not in c1:
                print u"\t+ %s" % nouns[int(i)]
            elif i not in c2:
                print u"\t- %s" % nouns[int(i)]
            else:
                print u"\t  %s" % nouns[int(i)]

def get_clusters(input_file):
    sim_dict = json.load(open(input_file, "r"))
    
    cl = KMeanCluster.build_clusters(sim_dict, len(sim_dict.keys()) / 10)

    return cl

def main():
    day1_file = sys.argv[1]
    day2_file = sys.argv[2]

    cl1 = get_clusters(day1_file) 
    cl2 = get_clusters(day2_file) 

    cur = stats.get_cursor(os.environ["MOLVA_DB"])
    nouns = stats.get_nouns(cur)

    compare_clusters(cl1, cl2, nouns)
    
if __name__ == "__main__":
    main()
