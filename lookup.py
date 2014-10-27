#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys
import os
import logging, logging.config
import json
import random
import codecs

import stats
import util

sys.stdout = codecs.getwriter('utf8')(sys.stdout)

logging.config.fileConfig("logging.conf")

settings = {} 
try:
    settings = json.load(open('global-settings.json', 'r'))
except Exception as e:
    logging.warn(e)

DB_DIR = settings["db_dir"] if "db_dir" in settings else os.environ["MOLVA_DIR"]

BLOCKED_NOUNS_LIST = u"\n".join(list(u"абвгдеёжзиклмнопрстуфхцчшщыьъэюя"))

BLOCKED_NOUNS = ",".join(map( lambda x: str(util.digest(x)), BLOCKED_NOUNS_LIST.split("\n")))

NOUNS_LIMIT = 2000
POST_MIN_FREQ = 10


def main():
    logging.info(u"По-русски")
    parser = util.get_dates_range_parser()
    args = parser.parse_args()

    cur = stats.get_cursor("%s/tweets_%s.db" % (DB_DIR, args.start ))
   
    sims = open(args.end,'r')
    nouns = stats.get_nouns(cur)

    valid = map(str,[3615091692, 3577221357, 3504712731  ])
    sim_keys = []
    while True:
        l = sims.readline()
        if l is None or l == '':
            break
        n1, n2, sim = l.split(';')
        #if n1 in valid and n2 in valid:
        #    print u"%s %s %s" % (nouns[int(n1)], nouns[int(n2)], float(sim.replace(',','.')))
        sim_keys.append(nouns[int(n1)])
        sim_keys.append(nouns[int(n2)])

    for s in set(sim_keys):
        print s

    return

            
if __name__ == '__main__':
    main()
