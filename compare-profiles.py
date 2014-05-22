#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys
import os
import logging, logging.config
import json
import argparse
import numpy
import codecs

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

codecs.getwriter('utf8')(sys.stdout)

def get_profiles(ind, date):
    cur = ind.get_db_for_date(date)

    nouns = stats.get_nouns(cur)
    logging.info("%s: nouns len %s" % (date, len(nouns)))

    profiles_dict = stats.setup_noun_profiles(cur, {}, nouns, post_min_freq = 10, blocked_nouns=BLOCKED_NOUNS, nouns_limit = 500)
    logging.info("%s: profiles len %s" % (date, len(profiles_dict)))

    return [profiles_dict, nouns]

def save_sims(cur, sims):
    cur.execute("begin transaction")

    for s in sims:
        cur.execute("replace into noun_sim_svd values (?, ?, ?)", s)

    cur.execute("commit")

def main():
    parser = util.get_dates_range_parser()
    args = parser.parse_args()

    ind = Indexer(DB_DIR)

    #prof1, nouns1 = get_profiles(ind, args.start)
    #prof2, nouns2 = get_profiles(ind, args.end)
   
    cur = ind.get_db_for_date(args.start)

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

    u, s, v = numpy.linalg.svd(m, full_matrices=False)

    k = 50

    uk = numpy.transpose(numpy.transpose(u)[:k])
    sk = s[:k]

    stats.create_given_tables(cur, ["noun_similarity"])
    cur.execute("create table if not exists noun_sim_svd as select * from noun_similarity limit 0")

    p_keys = prof.keys()

    sims = []
    for i in range(0, len(p_keys)):
        for j in range(i + 1, len(p_keys)):
            
            p1_ = map(lambda x: u[i][x] * sk[x] , range(0, k))
            p2_ = map(lambda x: u[j][x] * sk[x] , range(0, k))

            sim = numpy.dot(p1_, p2_) / (numpy.linalg.norm(p1_) * numpy.linalg.norm(p2_))
            sims.append((p_keys[i], p_keys[j], sim))
 
            if len(sims) > 20000:
                save_sims(cur, sims)
                sims = []

                logging.info("Another 10k seen")

    save_sims(cur, sims)

    logging.info("done")
    

if __name__ == '__main__':
    main()
