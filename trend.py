#!/usr/bin/python
import sys
import os
import logging, logging.config
import json
import codecs

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

def sum_cnt(line):
    return reduce(lambda z, y: z + y, line)

def main():
    parser = util.get_dates_range_parser()
    parser.add_argument("-o", "--out-file")
    args = parser.parse_args()


    ind = Indexer(DB_DIR)
    cur_main = stats.get_cursor(DB_DIR + "/tweets.db")


    noun_trends = {}   
    #noun_text = {}
    suffs = ("", "_n_1", "_n_2", "_n_3")
    for i in range(0, len(suffs)):
        suff = suffs[i]
        logging.info("Fetch post cnt for suff %s" % suff)
        cur_main.execute("""
            select post_md5, post_cnt
            from post_cnt%(suff)s
            order by post_cnt desc
        """ % {"suff": suff})

        while True:
            row = cur_main.fetchone()
            if row is None:
                break
            post_md5, post_cnt = row

            if post_md5 not in noun_trends:
                noun_trends[post_md5] = map(lambda x: 0, range(0, 4))
            noun_trends[post_md5][i] = post_cnt

        #logging.info("Update nouns dictionary")
        #nouns = stats.get_nouns(cur)
        #for n in nouns:
        #    noun_text[n] = nouns[n]

    logging.info("Done fetching post_cnt")

    noun_trends_data = []
    for noun in sorted(noun_trends.keys()):
        line = noun_trends[noun]      
        top, tail = (line[:-1], line[-1])
        mean = float(sum_cnt(top)) / len(top)
        if mean > 10:
            dev = (tail - mean) / mean if mean != 0 else 0

            noun_trends_data.append((noun, dev))
    
    stats.create_given_tables(cur_main, ["noun_trend"]) 

    cur_main.execute("begin transaction")
    cur_main.executemany("insert into noun_trend values (?, ?)", noun_trends_data)
    cur_main.execute("commit")

    logging.info("Done")

if __name__ == '__main__':
    main()
