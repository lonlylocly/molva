#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys
import os
import logging, logging.config
import json
import argparse
import numpy

import stats
from Indexer import Indexer
import util
from profile import NounProfile

logging.config.fileConfig("logging.conf")

settings = {} 
try:
    settings = json.load(open('global-settings.json', 'r'))
except Exception as e:
    logging.warn(e)

DB_DIR = settings["db_dir"] if "db_dir" in settings else os.environ["MOLVA_DIR"]

BLOCKED_NOUNS_LIST = u"\n".join(list(u"абвгдеёжзиклмнопрстуфхцчшщыьъэюя"))

BLOCKED_NOUNS = ",".join(map( lambda x: str(util.digest(x)), BLOCKED_NOUNS_LIST.split("\n")))

def get_profiles(ind, date):
    cur = ind.get_db_for_date(date)

    nouns = stats.get_nouns(cur)
    logging.info("%s: nouns len %s" % (date, len(nouns)))

    profiles_dict = stats.setup_noun_profiles(cur, {}, nouns, post_min_freq = 10, blocked_nouns=BLOCKED_NOUNS, nouns_limit = 1000)
    logging.info("%s: profiles len %s" % (date, len(profiles_dict)))

    return [profiles_dict, nouns]

def main():
    parser = util.get_dates_range_parser()
    args = parser.parse_args()

    ind = Indexer(DB_DIR)

    #prof1, nouns1 = get_profiles(ind, args.start)
    #prof2, nouns2 = get_profiles(ind, args.end)
   
    prof, nouns = get_profiles(ind, args.start)
    
    replys_v = set()
    for p in prof:
        replys_v |= set(prof[p].replys.keys())

    m = []
    for p in prof:
        m_i = []
        for r in replys_v:
            if r in prof[p].replys:
                m_i.append(prof[p].replys[r])
            else:
                m_i.append(0)
        m.append(m_i)

    logging.info("%s x %s" % (len(m), len(m[0])))

    res = numpy.linalg.svd(m, full_matrices=False)

    logging.info("done")
    

if __name__ == '__main__':
    main()
