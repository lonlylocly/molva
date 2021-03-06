#!/usr/bin/python
import sys
import os
import logging, logging.config
import json
from datetime import datetime, timedelta
import argparse

import molva.stats as stats
import molva.util as util

logging.config.fileConfig("logging.conf")

settings = {} 
try:
    settings = json.load(open('global-settings.json', 'r'))
except Exception as e:
    logging.warn(e)

DB_DIR = settings["db_dir"] if "db_dir" in settings else os.environ["MOLVA_DIR"]

MIN_TREND_FREQ = 10

def least_squares(series):
    n = len(series);
    xy_mean = 0.0
    x_mean = 0.0
    y_mean = 0.0
    x2_mean = 0.0
    for i in range(1, n+1):
        xy_mean += i*series[i-1]
        y_mean += series[i-1]
        x_mean += i
        x2_mean += i *i
    
    xy_mean = xy_mean / n
    x_mean = x_mean / n
    y_mean = y_mean /n
    x2_mean = x2_mean /n

    b = (xy_mean - x_mean * y_mean) / (x2_mean - x_mean * x_mean)
    a = y_mean - b * x_mean 

    #logging.info("A: %s; B: %s; x_mean: %s; y_mean: %s" % (a, b, x_mean, y_mean));   
 
    approx = []
    for i in (0, n-1):
        approx.append(a + b * i)
  
    return (a, b, approx) 

@util.time_logger
def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--db-dir", default=DB_DIR)
    args = parser.parse_args()

    cur_display = stats.get_cursor(args.db_dir + "/tweets_display.db")
    cur_main = stats.get_cursor(args.db_dir + "/tweets.db")
    #cur_main = stats.get_cursor(args.db_dir + "/tweets_20150221.db")
    #nouns = stats.get_nouns(cur_main)

    #logging.info(type(nouns.keys()[0]))

    utc_now = datetime.utcnow()
    date_3day = (utc_now - timedelta(3)).strftime("%Y%m%d%H%M%S")
    date_3day_tenminute = date_3day[:11]
    logging.info("Time left bound: %s" % date_3day_tenminute)
    hour_word_cnt = {}
    word_cnt = {}
    for day in [3, 2, 1, 0]:
        date = (utc_now - timedelta(day)).strftime("%Y%m%d")
        word_time_cnt_table = "word_time_cnt_%s" % date
        mcur = stats.get_mysql_cursor(settings)
        stats.create_mysql_tables(mcur, {word_time_cnt_table: "word_time_cnt"})
        mcur.execute("""
                select word_md5, substr(tenminute, 1, 10) as hour, sum(cnt) 
                from %s
                where tenminute > %s
                group by word_md5, hour
        """ % (word_time_cnt_table, date_3day_tenminute))
    
        row_cnt = 0    
        while True:
            res = mcur.fetchone()
            if res is None:
                break
            word_md5, hour, cnt = map(int,res)
            if hour not in hour_word_cnt:
                hour_word_cnt[hour] = {}
            hour_word_cnt[hour][word_md5] = cnt
            if word_md5 not in word_cnt:
                word_cnt[word_md5] = 0
            word_cnt[word_md5] += cnt
            
            row_cnt += 1
            if row_cnt % 100000 == 0:
                logging.info('Seen %s rows' % row_cnt)
   
    word_series = [] 
    hours = sorted(hour_word_cnt.keys())
    for word in word_cnt.keys():
        series = []
        series_max = 0
        for hour in hours:
            if word in hour_word_cnt[hour]:
                series.append(hour_word_cnt[hour][word])
                if hour_word_cnt[hour][word] > series_max:
                    series_max = hour_word_cnt[hour][word]
            else:
                series.append(0)
        # normalize by maxfreq in series 
        if series_max > 0:
            series = [ (float(x) / series_max) * 100 for x in series ]
        approx = least_squares(series)
        a, b, app_ser = approx
        word_series.append({
            "word_md5": word, 
            "word_cnt": word_cnt[word], 
            "line_c": a, 
            "slope": b, 
            "delta": app_ser[-1] - app_ser[0]
        })
    
    word_series = sorted(word_series, key=lambda x: x["slope"], reverse=True)[:2000] 

    for cur in [cur_main, cur_display]:
        stats.create_given_tables(cur, {"noun_trend_new": "noun_trend"})
        cur.execute("begin transaction")
        for s in word_series:
            cur.execute("insert into noun_trend_new values (%s, %s)" % (s["word_md5"], s["slope"]))

        cur.execute("drop table noun_trend")
        cur.execute("alter table noun_trend_new rename to noun_trend")
        cur.execute("commit")

    logging.info("Done")

if __name__ == '__main__':
    main()
