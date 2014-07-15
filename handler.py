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

settings = json.load(open('global-settings.json', 'r'))

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

    def get_clusters(self, skip):
        cur = stats.get_cursor(settings["db_dir"] + "/tweets_display.db") 
        res = cur.execute("""
            select cluster 
            from clusters 
            order by cluster_date desc 
            limit 1 
            offset %s
        """ % (skip)).fetchone()

        return res[0] 

    def get(self):
        skip = self.get_argument("skip", default=0)
        try:
            skip = int(skip)
        except:
            skip = 0

        cl = self.get_clusters(skip=skip)

        self.write(cl)

class TrendHandler(tornado.web.RequestHandler):
    def get_trends(self):

        cur = stats.get_cursor(settings["db_dir"] + "/tweets.db") 
        cur.execute("""
            select noun, t.noun_md5, trend, p.post_cnt
            from noun_trend t
            left join nouns n
            on t.noun_md5 = n.noun_md5
            left join post_cnt p
            on t.noun_md5 = p.post_md5
            order by trend desc 
        """)

        trends = []
        while True:
            r = cur.fetchone()
            if r is None:
                break
            noun, noun_md5, trend, post_cnt = r
            trends.append("".join(map(lambda x: "%20s" % x, [noun_md5, noun, trend, post_cnt])))
            #trends.append( {'noun': noun, 'noun_md5': noun_md5, 'trend': trend, 'post_cnt': post_cnt})

        return "<pre>" + "\n".join(trends) + "</pre>"

    def get(self):

        trends = self.get_trends()

        self.write(trends)
   
if __name__ == '__main__':
    application = tornado.web.Application([
        (r"/api/cluster", ClusterHandler),
        (r"/api/trend", TrendHandler),
    ])
    application.listen(8000)
    tornado.ioloop.IOLoop.instance().start()
