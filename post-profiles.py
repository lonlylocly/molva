#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys
import os
import logging, logging.config
import json

import molva.stats as stats
from Indexer import Indexer
import molva.util as util

logging.config.fileConfig("logging.conf")

POST_MIN_FREQ = 10

settings = {} 
try:
    settings = json.load(open('global-settings.json', 'r'))
except Exception as e:
    logging.warn(e)

DB_DIR = settings["db_dir"] if "db_dir" in settings else os.environ["MOLVA_DIR"]

def save_sims(cur, sims):
    cur.execute("begin transaction")

    cur.executemany("replace into noun_sim_new values (?, ?, ?)", sims)

    cur.execute("commit")


def main():
    logging.info("Start")

    parser = util.get_dates_range_parser()
    parser.add_argument("-i", "--in-file")
    args = parser.parse_args()

    ind = Indexer(DB_DIR)
    cur = stats.get_main_cursor(DB_DIR)
            
    stats.create_given_tables(cur, ["noun_similarity"])
    cur.execute("create table if not exists noun_sim_new as select * from noun_similarity limit 0")
    cur.execute("delete from noun_sim_new")

    in_file = open(args.in_file, 'r')
    sims = []
    for line in in_file:
        sims.append(line.split(";"))
        if len(sims) > 20000:
            save_sims(cur, sims)
            sims = []

    save_sims(cur, sims)

    cur.execute("begin transaction")

    cur.execute("delete from noun_similarity")
    cur.execute("insert or ignore into noun_similarity select * from noun_sim_new")

    cur.execute("commit")

    logging.info("Done")
        
if __name__ == '__main__':
    main()


