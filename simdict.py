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
import ctypes
import struct

sys.stdout = codecs.getwriter('utf8')(sys.stdout)

def save_noun(n1, val_list, sim_dict):
    buf = ctypes.create_string_buffer(len(val_list) * 8)
    offset = 0
    for i in val_list:
        struct.pack_into('If', buf, offset, i[0], i[1])
        offset += 8
    sim_dict[n1] = buf          


def get_dists(sim_file,max_dists=None):
    print "[%s] get_dists startup" % time.ctime() 

    cnt = 0
    log_cnt = 0
    
    sim_f = open(sim_file, 'r')

    sim_dict = {}
    val_list = []
    last_n1 = 0
    while True:
        l = sim_f.readline()
        if l is not None and l != '':
            n1, n2, sim = l.split("\t")        
            #dist = int((1.0 - float(sim)) * 1e6)
            dist = 1.0 - float(sim)
            n1 = int(n1)
            n2 = int(n2)
            if n1 != last_n1:
                if last_n1 != 0:
                    save_noun(last_n1, val_list, sim_dict)
                last_n1 = n1
                val_list = [(n2, dist)]
            else:
                val_list.append((n2, dist))
        else:
            save_noun(last_n1, val_list, sim_dict)
            break

        cnt = cnt + 1 
        if cnt > log_cnt * 5e5:
            log_cnt = log_cnt + 1
            print "[%s] Done so far %s" %(time.ctime(), cnt)   
            if max_dists is not None and cnt > max_dists:
                break

    print "[%s] get_dists done. Count %s" %(time.ctime(), cnt)   

    return sim_dict 

def unpack_lev2(buf):
    assert len(buf) % 8 == 0
    n2_dict = {}
    for i in range(0, len(buf)/8):
        n2, dist = struct.unpack_from("If", buf, i*8)
        n2_dict[n2] = dist
    return n2_dict
    
def get_dist(n1, n2, sim_dict):
    ret_val = 1.0 if n1 != n2 else 0.0 

    lev1 = n1 if n1 < n2 else n2
    lev2 = n2 if n1 < n2 else n1

    if lev1 in sim_dict:
        lev2_dict = unpack_lev2(sim_dict[lev1])
        if lev2 in lev2_dict:
            ret_val = lev2_dict[lev2]

    return ret_val

