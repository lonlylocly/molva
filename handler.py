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
from datetime import datetime

import KMeanCluster
import stats
from Indexer import Indexer

#logging.config.fileConfig("logging.conf")

settings = json.load(open('global-settings.json', 'r'))

class ClusterHandler(tornado.web.RequestHandler):

    def get_clusters(self, skip, before, date):
        cur = stats.get_cursor(settings["db_dir"] + "/tweets_display.db") 
        if date is not None:
            cur.execute("""
                select cluster 
                from clusters 
                where cluster_date = '%(date)s'
            """  % ({'date': date}))
        elif before is not None:
            cur.execute("""
                select cluster 
                from clusters 
                where cluster_date < '%(before)s'
                order by cluster_date desc 
                limit 1 
            """ % ({'before': before}))
        else:
            cur.execute("""
                select cluster 
                from clusters 
                order by cluster_date desc 
                limit 1 
                offset %s
            """ % (skip))
        res = cur.fetchone()[0] 

        return res

    def parse_date(self, mydate):
        if mydate is None or mydate == "":
            return None
        try:
            mydate = mydate.replace("-","").replace(" ","").replace(":","") 

            unixtime = datetime.strptime(mydate, "%Y%m%d%H%M%S").strftime("%s")
            mydate_dt = datetime.utcfromtimestamp(int(unixtime))
            mydate = mydate_dt.strftime("%Y%m%d%H%M%S")
            
            return mydate
        except Exception as e:
            logging.info(e)
            return None

    def get(self):
        skip = self.get_argument("skip", default=0)
        before = self.parse_date(self.get_argument("before", default=None))
        date = self.parse_date(self.get_argument("date", default=None))
        try:
            skip = int(skip)
        except:
            skip = 0

        logging.info("Before %s (UTC)" % before)
        logging.info("Date %s (UTC)" % before)

        cl = self.get_clusters(skip, before, date)

        self.write(cl)

   
if __name__ == '__main__':
    application = tornado.web.Application([
        (r"/api/cluster", ClusterHandler)
    ])
    application.listen(8000)
    tornado.ioloop.IOLoop.instance().start()
