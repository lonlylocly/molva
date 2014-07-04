#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys
import os
import logging, logging.config
import json
from datetime import datetime

import stats
from Indexer import Indexer
import util
import CMeansCluster
from util import digest

logging.config.fileConfig("logging.conf")

POST_MIN_FREQ = 10

settings = {} 
try:
    settings = json.load(open('global-settings.json', 'r'))
except Exception as e:
    logging.warn(e)

DB_DIR = settings["db_dir"] if "db_dir" in settings else os.environ["MOLVA_DIR"]

BLOCKED_NOUNS_LIST = u"\n".join(list(u"абвгдеёжзиклмнопрстуфхцчшщыьъэюя"))

BLOCKED_NOUNS = ",".join(map( lambda x: str(digest(x)), BLOCKED_NOUNS_LIST.split("\n")))

NOUNS_LIMIT = 200

def main():
    logging.info("start")
    parser = util.get_dates_range_parser()
    args = parser.parse_args()

    ind = Indexer(DB_DIR)
    cur = stats.get_cursor(DB_DIR + "/tweets.db")
            
    profiles_dict = stats.setup_noun_profiles(cur, {}, {}, 
        post_min_freq = POST_MIN_FREQ, blocked_nouns = BLOCKED_NOUNS, nouns_limit = NOUNS_LIMIT 
    )

    logging.info("profiles len %s" % len(profiles_dict))

    cMeans = CMeansCluster.CMeansCluster(map(lambda x: profiles_dict[x], profiles_dict.keys()), 100)
    cMeans.cluster()


if __name__ == '__main__':
    main()


