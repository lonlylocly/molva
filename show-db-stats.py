#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys
import os
import logging, logging.config
import json

import stats
from Indexer import Indexer
import util

logging.config.fileConfig("logging.conf")

settings = {} 
try:
    settings = json.load(open('global-settings.json', 'r'))
except Exception as e:
    logging.warn(e)

DB_DIR = settings["db_dir"] if "db_dir" in settings else os.environ["MOLVA_DIR"]

def print_cols(arr, per_col=20):
    print "".join(map(lambda x: "%16s" % x, arr))

def main():
    parser = util.get_dates_range_parser()
    args = parser.parse_args()

    logging.info("Start")
    ind = Indexer(DB_DIR)

    cur_main = stats.get_cursor(DB_DIR + "/tweets.db")
    stats.create_given_tables(cur_main, ["table_stats"])

    db_stats = {}   
    for date in sorted(ind.dates_dbs.keys()):
        if args.start is not None and date < args.start:
            continue
        if args.end is not None and date > args.end:
            continue
        db_stats[date] = {}
        cur = ind.get_db_for_date(date)
        #try:
        tables = cur.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        for t in tables:
            table = t[0]
            cnt = cur.execute("select count(*) from %s" % table).fetchone() 
            logging.info("Table: %s; count: %s" % (table, cnt[0]))
            db_stats[date][table] = cnt[0]
            cur_main.execute("""
                replace into table_stats (table_name, table_date, row_count, update_time) 
                values (?, ?, ?, datetime(current_timestamp, 'localtime'))
            """, (table, date, cnt[0])) 
        #except Exception as ex:
        #    logging.error(ex)
        #    continue
       
    tables = [] 
    dates = sorted(db_stats)
    for date in dates:
        tables += db_stats[date].keys()
    
    tables = list(set(tables))

    print_cols(["table"] + dates)

    for table in tables:
        r = [table] 
        for date in dates:
            r.append(str(db_stats[date][table]) if table in db_stats[date] else "")

        print_cols(r)

if __name__ == '__main__':
    main()


