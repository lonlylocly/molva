#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys
import os
import logging, logging.config
import json
from datetime import datetime, timedelta
import math
import argparse
import traceback
import codecs

import stats
from Indexer import Indexer
import util

logging.config.fileConfig("logging.conf")
logging.getLogger().setLevel(logging.INFO)

settings = {} 
try:
    settings = json.load(open('global-settings.json', 'r'))
except Exception as e:
    logging.warn(e)

DB_DIR = settings["db_dir"] if "db_dir" in settings else os.environ["MOLVA_DIR"]

def get_cluster_nouns(clusters):
    cl_nouns = []
    for c in clusters:
        for m in c["members"]:
            cl_nouns.append(int(m["id"]))

    return cl_nouns

@util.time_logger
def get_word_stats(clusters):
    cl_nouns = get_cluster_nouns(clusters)
    cl_nouns_joined = ",".join(map(str, cl_nouns))

    utc_now = datetime.utcnow()
    date_1day = (utc_now - timedelta(1)).strftime("%Y%m%d%H%M%S")
    date_1day_tenminute = date_1day[:11]

    word_cnt = {}
    for day in [1, 0]:
        date = (utc_now - timedelta(day)).strftime("%Y%m%d")
        word_time_cnt_table = "word_time_cnt_%s" % date
        mcur = stats.get_mysql_cursor(settings)
        stats.create_mysql_tables(mcur, {word_time_cnt_table: "word_time_cnt"})
        mcur.execute("""
                select word_md5, sum(cnt) 
                from %s
                where tenminute > %s
                and word_md5 in (%s)
                group by word_md5
        """ % (word_time_cnt_table, date_1day_tenminute, cl_nouns_joined))

        row_cnt = 0    
        while True:
            res = mcur.fetchone()
            if res is None:
                break
            word_md5, cnt = map(int,res)
            if word_md5 not in word_cnt:
                word_cnt[word_md5] = 0
            word_cnt[word_md5] += cnt
            
            row_cnt += 1
            if row_cnt % 100000 == 0:
                logging.info('Seen %s rows' % row_cnt)

    return word_cnt

@util.time_logger
def get_bigram_stats(clusters, word_stats):
    cl_nouns = get_cluster_nouns(clusters)
    cl_nouns_joined = ",".join(map(str, cl_nouns))

    utc_now = datetime.utcnow()
    date_1day = (utc_now - timedelta(1)).strftime("%Y%m%d%H%M%S")
    date_1day_tenminute = date_1day[:11]

    bigram_cnt = {}
    for day in [1, 0]:
        date = (utc_now - timedelta(day)).strftime("%Y%m%d")
        bigram_table = "bigram_%s" % date
        mcur = stats.get_mysql_cursor(settings)
        stats.create_mysql_tables(mcur, {bigram_table: "bigram_day"})
        mcur.execute("""
                select source1, word1, source2, word2, sum(cnt) 
                from %s
                where tenminute > %s
                and word1 in (%s) and word2 in (%s)
                group by source1, word1, source2, word2
        """ % (bigram_table, date_1day_tenminute, cl_nouns_joined,
            cl_nouns_joined))

        row_cnt = 0    
        while True:
            res = mcur.fetchone()
            if res is None:
                break
            s1, w1, s2, w2, cnt = map(int,res)
            bigram = (s1, w1, s2, w2)
            if bigram not in bigram_cnt:
                bigram_cnt[bigram] = 0
            bigram_cnt[bigram] += cnt
            
            row_cnt += 1
            if row_cnt % 100000 == 0:
                logging.info('Seen %s rows' % row_cnt)

    bigram_list = []
    skipped_cnt = 0
    for bigram in bigram_cnt:
        s1, w1, s2, w2 = bigram
        cnt = bigram_cnt[bigram]
        cnt_ratio = float(cnt) / word_stats[w1]
        if cnt_ratio < settings["wordstats_noise_treshold"]:
            skipped_cnt += 1
            continue
        item = {
            "source1": s1, "word1": w1, "source2": s2, "word2": w2, "count": cnt,
            "word1_count": word_stats[w1]
        }
        bigram_list.append(item)

    logging.info("Skipped noisy bigrams: %d; noise treshold: %f" % (skipped_cnt, settings["wordstats_noise_treshold"]))

    return bigram_list

def get_nouns(clusters):
    cur = stats.get_main_cursor(DB_DIR)
    cl_nouns = get_cluster_nouns(clusters)

    nouns = stats.get_nouns(cur, cl_nouns)

    return nouns

@util.time_logger
def get_sources(bigram_stats):
    cur = stats.get_main_cursor(DB_DIR)

    source_ids = set()
    for item in bigram_stats:
        source_ids.add(item["source1"])
        source_ids.add(item["source2"])

    sources = stats.get_sources(cur, source_ids)

    return sources

def apply_word_text(bigram_stats, nouns, sources):
    bigram_stats2 = []
    duplicate_cnt = 0
    for item in bigram_stats:
        if item["word1"] == item["word2"]:
            duplicate_cnt += 1
        item2 = {
            "s1": {
                "sourceMd5": item["source1"],
                "text": sources[item["source1"]],
                "word": {
                    "wordMd5": item["word1"],
                    "text": nouns[item["word1"]],
                    "count": item["word1_count"]
                }
            },
            "s2": {
                "sourceMd5": item["source2"],
                "text": sources[item["source2"]],
                "word": {
                    "wordMd5": item["word2"],
                    "text": nouns[item["word2"]]
                }
            },
            "count": item["count"]
        }
        bigram_stats2.append(item2)

    logging.info("Duplicate cnt: %d" % duplicate_cnt)

    return bigram_stats2

@util.time_logger
def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--clusters")
    parser.add_argument("--out-bigram-stats")
    args = parser.parse_args()

    cur_display = stats.get_cursor(DB_DIR + "/tweets_display.db")

    cl = json.load(open(args.clusters,'r'))

    word_stats = get_word_stats(cl)
    bigram_stats = get_bigram_stats(cl, word_stats)

    nouns = get_nouns(cl)
    sources = get_sources(bigram_stats)

    bigram_stats2 = apply_word_text(bigram_stats, nouns, sources)    

    logging.info("Got stats for: %d words; %d bigrams" % (len(word_stats), len(bigram_stats)))

    json.dump(bigram_stats2, codecs.open(args.out_bigram_stats, 'w', encoding="utf8"), indent=2, ensure_ascii=False)
     

if __name__ == '__main__':
    main()


