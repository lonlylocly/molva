#!/usr/bin/python
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


def main():
    parser = util.get_dates_range_parser()
    args = parser.parse_args()

    ind = Indexer(DB_DIR)
 
    for date in sorted(ind.dates_dbs.keys()):
        if args.start is not None and date < args.start:
            continue
        if args.end is not None and date > args.end:
            continue

        cur = ind.get_db_for_date(date)
        
        res = cur.execute("select id, noun_md5 from tweets_nouns").fetchall()

        tw_context = {}
        for r in res:
            tw_id, noun_md5 = r
            if tw_id not in tw_context:
                tw_context[tw_id] = []
            tw_context[tw_id].append(noun_md5)

        post_context = {}
        for t in tw_context:
            con = tw_context[t]
            for i in range(0, len(con)):
                for j in range(i, len(con)):
                    p1 = con[i]
                    p2 = con[j]
                    if p1 not in post_context:
                        post_context[p1] = {}
                    if p2 not in post_context[p1]:
                        post_context[p1][p2] = 0 
                    post_context[p1][p2] += 1 

        logging.info(len(post_context))

        stats.create_given_tables(cur, ["post_reply_cnt"])
        cur.execute("drop table if exists post_context_cnt")
        cur.execute("create table if not exists post_context_cnt as select * from post_reply_cnt limit 0")

        logging.info("prepare data to insert")
        data = []
        for p in post_context:
            for p2 in post_context[p]:
                data.append((p, p2, post_context[p][p2]))

        logging.info("insert data")
        cur.execute("begin transaction")
        cur.executemany("insert into post_context_cnt values (?, ?, ?)" , data)
        cur.execute("commit")

        logging.info("done")

if __name__ == '__main__':
    main()
