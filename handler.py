#!/usr/bin/python
# -*- coding: UTF-8 -*-
import tornado.ioloop
import tornado.web
import sys
import os
import json
import time
import re
import logging
from subprocess import check_output
import sqlite3

import KMeanCluster
import stats
from StatsDisplay import StatsDisplay
from Indexer import Indexer

#logging.config.fileConfig("logging.conf")

settings = json.load(open('handler-settings.json', 'r'))

class ClusterHandler(tornado.web.RequestHandler):

    def get_sims_for_date(self, date):
        cur = self.ind.get_db_for_date(date)
        res = cur.execute("select post1_md5, post2_md5, sim from noun_similarity")

        sim_dict = {}
        while True:
            r = cur.fetchone()
            if r is None:
                break
            p1, p2, sim = r
            if p1 not in sim_dict:
                sim_dict[p1] = {}
            if p2 not in sim_dict:
                sim_dict[p2] = {}
            sim_dict[p1][p2] = sim
            sim_dict[p2][p1] = sim
         
        for p in sim_dict.keys():
            sim_dict[p][p] = 0

        return sim_dict
 
    def get_nouns_for_date(self, date):
        cur = self.ind.get_db_for_date(date)
        return stats.get_nouns(cur)

    def get_clusters(self):
        cur = stats.get_cursor(settings["db_dir"] + "/tweets_display.db") 
        res = cur.execute("select cluster from clusters order by cluster_date desc limit 1 ").fetchone()

        return res[0]

    def get(self):

        cl = self.get_clusters()

        self.write(cl)

if __name__ == '__main__':
    application = tornado.web.Application([
        (r"/api/cluster", ClusterHandler),
    ])
    application.listen(8000)
    tornado.ioloop.IOLoop.instance().start()
