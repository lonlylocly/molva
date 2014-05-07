#!/usr/bin/python
# -*- coding: UTF-8 -*-
from urlparse import urlparse, parse_qs
import tornado.ioloop
import tornado.web
import sys
import os
import json

import KMeanCluster
import stats

settings = json.load(open('handler-settings.json', 'r'))

class ClusterHandler(tornado.web.RequestHandler):
    def initialize(self):
        self.sim_dict = json.load(open(settings["workdir"] + "/sims.json", "r"))
        cur = stats.get_cursor(settings["db"])
        self.nouns = stats.get_nouns(cur)

    def get(self):
        k = self.get_argument('k',default='100')
        cl = KMeanCluster.get_clusters(self.sim_dict, int(k), self.nouns)

        self.write(json.dumps(cl, indent=4))

if __name__ == '__main__':
    application = tornado.web.Application([
        (r"/cluster", ClusterHandler),
    ])
    application.listen(8000)
    tornado.ioloop.IOLoop.instance().start()
