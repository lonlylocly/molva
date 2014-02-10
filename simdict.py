#!/usr/bin/python
# -*- coding: utf-8 -*-
import sqlite3
from subprocess import Popen, PIPE, STDOUT
import sys,codecs
import re
import time
from util import digest
from subprocess import call
import os

sys.stdout = codecs.getwriter('utf8')(sys.stdout)



def get_dists(sim_file):
    print "[%s] get_dists startup" % time.ctime() 

    cnt = 0
    log_cnt = 0
    
    sim_f = open(sim_file, 'r')

    sim_dict = {}
    while True:
        l = sim_f.readline()
        if l is not None and l != '':
            n1, n2, sim = l.split("\t")        
            dist = 1.0 - float(sim)
            n1 = int(n1)
            n2 = int(n2)
            if n1 in sim_dict:
                sim_dict[n1][n2] = dist 
            else:
                sim_dict[n1] = {n2: dist}
        else:
            break

        cnt = cnt + 1 
        if cnt > log_cnt * 1e6:
            log_cnt = log_cnt + 1
            print "[%s] Done so far %s" %(time.ctime(), cnt)   

    print "[%s] get_dists done. Count %s" %(time.ctime(), cnt)   

    return sim_dict

def get_dist(n1, n2, sim_dist):
    ret_val = 1.0
    if n1 < n2:
        if n1 in sim_dist and n2 in sim_dist[n1]:
            ret_val =  sim_dist[n1][n2]
    elif n1 == n2:
        ret_val = 0.0
    else:
        if n2 in sim_dist and n1 in sim_dist[n2]:
            ret_val = sim_dist[n2][n1]

    #print "Dist %s, %s = %s" % (n1, n2, ret_val)

    return ret_val

