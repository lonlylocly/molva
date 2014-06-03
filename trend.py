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

    noun_trends = {}   
    noun_text = {}
    valid_dates = filter(lambda x: x <= args.end and x >= args.start, sorted(ind.dates_dbs.keys())) 
    for i in range(0, len(valid_dates)):
        date = valid_dates[i]
        cur = ind.get_db_for_date(date)
        logging.info("Fetch post cnt for date %s" % date)
        cur.execute("""
            select post_md5, post_cnt
            from post_cnt
            order by post_cnt desc
        """)

        while True:
            row = cur.fetchone()
            if row is None:
                break
            post_md5, post_cnt = row

            if post_md5 not in noun_trends:
                noun_trends[post_md5] = map(lambda x: 0, range(0, len(valid_dates)))
            noun_trends[post_md5][i] = post_cnt

        logging.info("Update nouns dictionary")
        nouns = stats.get_nouns(cur)
        for n in nouns:
            noun_text[n] = nouns[n]

    logging.info("write stats to file")

    noun_trends_data = []
    for noun in sorted(noun_trends.keys()):
        line = noun_trends[noun]      
        top, tail = (line[:-1], line[-1])
        mean = float(sum_cnt(top)) / len(top)
        if mean > 10:
            dev = (tail - mean) / mean if mean != 0 else 0
            #line.append(dev)
            #noun_trends2[noun] = line
            noun_trends_data.append((noun, dev))
    
    stats.create_given_tables(cur, ["noun_trend"]) 

    cur.execute("begin transaction")
    cur.executemany("insert into noun_trend values (?, ?)", noun_trends_data)
    cur.execute("commit")
 
    #f = codecs.open(args.out_file, 'w', encoding='cp1251')
    ##for noun in sorted(noun_trends.keys(), key=lambda x: sum_cnt(noun_trends[x]), reverse=True):
    #for noun in sorted(noun_trends2.keys(), key=lambda x: noun_trends2[x][-1], reverse=True):
    #    line = [noun_text[noun]] + map(str, noun_trends2[noun])
    #    f.write(";".join(line))         
    #    f.write(";\n")

    #f.close()

    logging.info("Done")

if __name__ == '__main__':
    main()
