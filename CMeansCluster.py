#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys,codecs
import time
import json
import os
import random
import logging
import math

import stats
from profile import NounProfile

sys.stdout = codecs.getwriter('utf8')(sys.stdout)


class CMeansCluster:

    def __init__(self, posts, k):
        self.posts = posts
        self.n = len(posts)
        self.k = k
        self.cs = [] 
        self.ws = [] 
        for i in range(0, len(posts)):
            self.ws.append(map(lambda x: random.random(), range(0, k)))

    def recount_centers(self):
        logging.info("Start")
        new_cs = [] 
        for i in range(0, self.k):
            new_c = NounProfile(-1 * i)
            for j in range(0, self.n):
                new_c.add_profile(self.posts[j], self.ws[j][i])                
            new_cs.append(new_c)

        self.cs = new_cs
        logging.info("stop")

    def recount_weights(self):
        logging.info("Start")
        new_ws = []
        error = 0
        for p_i in range(0, self.n):
            new_ws.append([])
            logging.info("Done %s"%p_i)
            for c_i in range(0, self.k):
                logging.info("ci %s"%(c_i))
                d_c_i = self.cs[c_i].compare_with(self.posts[p_i])
                w = 0
                for c_j in range(0, self.k):
                    d_c_j = self.cs[c_j].compare_with(self.posts[p_i])
                    w += ((1- d_c_i.dist) / (1 - d_c_j.dist)) ** 2
                w = 1 / w if w > 0 else w
                new_ws[p_i].append(w)
                error += math.fabs(w - new_ws[p_i][c_i]) 

        self.ws = new_ws
        logging.info("error %s" % error)
        logging.info("stop")

        return error

    def cluster(self):
        logging.info("start")
        error = 1
        while error > 0.01:
            self.recount_centers()
            error = self.recount_weights()

        logging.info("stop")
