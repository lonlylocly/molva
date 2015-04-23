#!/usr/bin/python
# -*- coding: utf-8 -*-
import json
from datetime import datetime as d
import logging
import argparse

import stats
import util

logging.config.fileConfig("logging.conf")

settings = {} 
try:
    settings = json.load(open('global-settings.json', 'r'))
except Exception as e:
    logging.warn(e)

@util.time_logger
def word_series_raw(mcur, date, words):
    mcur.execute("""
        SELECT word_md5, tenminute, cnt
        FROM word_time_cnt_%s
        WHERE word_md5 in (%s) 
  
    """ % (date, ",".join(words)))
  
    res = mcur.fetchall()

    return res

@util.time_logger
def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--date")
    parser.add_argument("--output")
    args = parser.parse_args()

    mcur = stats.get_mysql_cursor(settings)
    cur = stats.get_cursor(settings["db_dir"] + "/tweets_display.db")
    cur_main = stats.get_cursor(settings["db_dir"] + "/tweets.db")

    cur.execute("""
        select noun_md5, trend
        from noun_trend 
    """)
    
    res = cur.fetchall()
    word_series = {}
    for r in res:
        w, t = r
        word_series[str(w)]= {"trend": t, "word_md5": str(w)}

    word_text = stats.get_nouns(cur_main, word_series.keys()) 
    for w in word_series:
        word_series[w]["text"] = word_text[int(w)]

    tenminutes = []  
    res = word_series_raw(mcur,  args.date, word_series.keys())
    for r in res:
        w, t, c = r
        if t not in tenminutes:
            tenminutes.append(t)
    tenminutes = sorted(tenminutes)
    for w in word_series:
        word_series[w]["series"] = map(lambda x: 0, range(0, len(tenminutes)))
    for r in res:
        w, t, c = r
        t_id = tenminutes.index(t)
        word_series[str(w)]["series"][t_id] = c

    word_series_arr = []
    for w in word_series:
        word_series_arr.append(word_series[w])
    logging.info("Write output to: %s" % args.output)
    json.dump(word_series_arr, open(args.output, 'w')) 
    
    
if __name__ == '__main__':
    main()
