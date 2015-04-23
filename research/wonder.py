#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys,codecs
import os
import logging, logging.config
import json
import re
import math

import stats
from Indexer import Indexer
import util

logging.config.fileConfig("logging.conf")

sys.stdout = codecs.getwriter('utf8')(sys.stdout)

settings = {} 
try:
    settings = json.load(open('global-settings.json', 'r'))
except Exception as e:
    logging.warn(e)

DB_DIR = settings["db_dir"] if "db_dir" in settings else os.environ["MOLVA_DIR"]

def execute(cur, query):
#    logging.debug(query)
    cur.execute(query)

def least_square(points):
    sum_x = 0.0
    sum_x2 = 0.0
    sum_y = 0.0
    sum_xy = 0.0
    n = len(points)
    for i in range(0, n):
        x = i+1
        y = points[i]
        sum_x += x
        sum_y += y
        sum_x2 += x*x
        sum_xy += x * y

    m = (sum_xy - sum_x * sum_y / n) / (sum_x2 - sum_x * sum_x / n)

    return m

def main():
    logging.basicConfig(level="INFO")
    parser = util.get_dates_range_parser()
    args = parser.parse_args()
    ind = Indexer(DB_DIR)
    cur_date = ind.get_db_for_date(args.start)

    cur_main = stats.get_main_cursor(DB_DIR)
    cur_display = stats.get_cursor(DB_DIR + "/tweets_display.db")

    cur_display.execute("""
        select cluster 
        from clusters 
        order by cluster_date desc
    """) 

    clusters = json.loads(cur_display.fetchone()[0])

    statistics = []
    for cl in clusters["clusters"]:
        logging.info("Got another cluster")
        logging.info("Len: %s" % len(cl["members"]))
        members = map(lambda x: str(x["id"]), cl["members"])
        members_text = map(lambda x: x["text"], cl["members"])
        logging.info(u" ".join(members_text))

        aggrs = []
        for i in range(1, len(members) + 1):
            aggrs.append("""
                sum(case cnt 
                    when %s then 1 else 0 end) sum_%s
            """ % (i, i))
        execute(cur_date, """
            select 
                count(id), 
                %(aggrs)s
            from (
                select id, count() as cnt
                from tweets_nouns
                where noun_md5 in (%(mems)s)
                group by id
            )
        """ % {"aggrs": ",".join(aggrs), "mems": ",".join(members)})
    
        res = cur_date.fetchone()

        logging.info("\t".join(map(str,list(res)[1:])))

        stat = []
        baseline = float(res[1]) + 1
        for i in list(res)[1:]:
            stat.append( - math.log((i+1) / baseline , 2))
            if i == 0:
                break

        logging.info("\t".join(map(str,stat)))
        logging.info(least_square(stat))
        statistics.append({"text": "\t".join(members_text), "total": res[0], "robustness": least_square(stat)})
        #logging.info("Average coocurence frequency: %.3f" % (  (float(tot_cnt)/len(members)/tot_ids)))
        

    for x in sorted(statistics, key=lambda x: x["robustness"]):
        print u"%.5s %5s %s" % (x["robustness"], x["total"], x["text"])

if __name__ == '__main__':
    main()
