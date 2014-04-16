#!/usr/bin/python
# -*- coding: utf-8 -*-
import sqlite3
from subprocess import Popen, PIPE, STDOUT
import sys,codecs
import re
import time
from util import digest

sys.stdout = codecs.getwriter('utf8')(sys.stdout)

db = 'more_replys2.db'

def print_stats(stat):
    for i in range(0,11):
        print "%d: %d"%(i, stat[i])

def main():
    print "[%s] Startup" % time.ctime() 

    stat = {}
    for x in range(0,11): 
        stat[ x] = 0

    print stat 
    sim_f = open('_noun_sim.dump', 'r')
    sim_fr = open('_noun_sim_reduced.dump', 'w')
    cnt = 0
    log_cnt = 0
    while True:
        l = sim_f.readline()
        if l is None or l == '':
            break
        n1, n2, sim = l.split("\t")
        magn = int(float(sim) * 10)
        stat[magn] = stat[magn] + 1
        if magn > 0 and magn < 10:
            sim_fr.write( l+"\n")
        cnt = cnt + 1
        if cnt > log_cnt * 1e7:
            print "[%s] Count %s" %(time.ctime(), cnt)   
            log_cnt = log_cnt + 1
            print_stats(stat)
    print "[%s] Count %s" %(time.ctime(), cnt)   
    print_stats(stat)

if __name__ == "__main__":
    main()
