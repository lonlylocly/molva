#!/usr/bin/python
import sys
import os
import logging, logging.config
import json
from datetime import datetime, timedelta, date
import math

import molva.stats as stats
from molva.Indexer import Indexer
import molva.util as util
from molva.Fetcher import to_mysql_timestamp

logging.config.fileConfig("logging.conf")

settings = {} 
try:
    settings = json.load(open('global-settings.json', 'r'))
except Exception as e:
    logging.warn(e)

DB_DIR = settings["db_dir"] if "db_dir" in settings else os.environ["MOLVA_DIR"]




def _print_counts(cur, t):
    cnt = cur.execute("select count(*) from %s" % t).fetchone()[0]
    logging.info("count(*) from %s = %s" % (t, cnt))

def attach_db(cur, db_file, db_name):
    query = "attach '%s' as %s" % (db_file, db_name)
    logging.info(query)
    cur.execute(query)

@util.time_logger
def write_post_reply_cnt(cur, mcur):
    mcur.execute("select word1, word2, cnt from word_mates_tmp")

    logging.info("Filling post_reply_cnt")
    cur.execute("begin transaction")
    while True:
        r = mcur.fetchone()        
        if r is None:
            break
        w1, w2, cnt = r
        cur.execute("""
            insert into post_reply_cnt (post_md5, reply_md5, reply_cnt)
            values (%s, %s, %s)
        """ % r)
        if w1 != w2:
            cur.execute("""
                insert into post_reply_cnt (post_md5, reply_md5, reply_cnt)
                values (%s, %s, %s)
            """ % (w2, w1, cnt))
           
    cur.execute("commit")

@util.time_logger
def count_currents2(cur, mcur, utc_now, words):
    yesterday = (utc_now - timedelta(1)).strftime("%Y%m%d")          
    today = (utc_now).strftime("%Y%m%d")    

    utc_ystd = (utc_now - timedelta(2)).strftime("%Y%m%d%H%M%S")
    utc_ystd_tenminute = utc_ystd[:11]

    table1 = "word_mates_" + yesterday 
    table2 = "word_mates_" + today
    table_tmp = "word_mates_tmp"

    mcur.execute("DROP TABLE IF EXISTS %s" % table_tmp)    

    stats.create_mysql_tables(mcur, {
        table1: "word_mates",
        table2: "word_mates",
        table_tmp: "word_mates_sum"
    })

    for t in [table1, table2]:
        query = """
            INSERT INTO word_mates_tmp 
            (word1, word2, cnt)
                SELECT word1, word2, sum(cnt) 
                FROM %s 
                WHERE word1 IN ( %s ) 
                AND tenminute > %s
                GROUP BY word1, word2
            ON DUPLICATE KEY UPDATE
            cnt = cnt + VALUES(cnt) 
        """ % (t, ",".join(words), utc_ystd_tenminute)
        #query = """
        #    INSERT INTO word_mates_tmp 
        #    (word1, word2, cnt)
        #        SELECT word1, word2, sum(cnt) 
        #        FROM %s 
        #        WHERE tenminute > %s
        #        GROUP BY word1, word2
        #    ON DUPLICATE KEY UPDATE
        #    cnt = cnt + VALUES(cnt) 
        #""" % (t, utc_ystd_tenminute)


        logging.debug(query)
        mcur.execute(query)

    write_post_reply_cnt(cur, mcur)
    
    for t in ["post_cnt", "post_reply_cnt"]:
        _print_counts(cur, t) 

@util.time_logger
def save_word_cnt(cur, word_cnt_tuples):
    cur.execute("begin transaction")
    cur.executemany("insert into post_cnt values (?, ?)", word_cnt_tuples)    
    cur.execute("commit")

class Rank:
    def __init__(self, value):
        self.value = value
        self.rank = None

    def __str__(self):
        return "value: %s; rank: %s" % (self.value, self.rank)

    @staticmethod
    def weight_ranks(ranks):
        cur_rank = 0
        for rank in sorted(ranks, key=lambda x: x.value, reverse=True) :
            rank.rank = cur_rank
            cur_rank += 1       

class WordCombinedRank:

    def __init__(self, word, cnt=0, trend=0):
        self.word = word
        self.cnt = Rank(cnt)
        self.trend = Rank(trend)

    def __str__(self):
        return "%s; cnt: %s; trend: %s" % (self.word, self.cnt, self.trend)

@util.time_logger
def make_tf_idf_ranks(word_cnt_tuples):
    word_ranks = {}
    N = 4e6
    #for (word, cnt) in word_cnt_tuples:
    #    N += cnt
    logging.info("TF-IDF scoring  N=%d" % N)
    for w in word_cnt_tuples:
        word, cnt = w
        idf = math.log(float(N) / float(cnt))
        tf = 1 + math.log(cnt)
        word_ranks[word] = WordCombinedRank(word, cnt=tf*idf)     

    return word_ranks

@util.time_logger
def get_trending_words(db_dir, word_cnt_tuples):
    cur = stats.get_cursor(db_dir + "/tweets_display.db")

    stats.create_given_tables(cur, ["noun_trend"])
    cur.execute("""
        select noun_md5, trend 
        from noun_trend
        order by trend desc
        limit 2000
    """)
    word_trends = map(lambda x: (int(x[0]), float(x[1])), cur.fetchall())

    word_ranks = make_tf_idf_ranks(word_cnt_tuples)

    for w in word_trends:
        word, trend = w
        if word not in word_ranks:
            logging.warn("No such word_md5 at word_ranks %s" % word)
            continue
        word_ranks[word].trend.value = trend

    Rank.weight_ranks(map(lambda x: x.trend, word_ranks.values()))
    Rank.weight_ranks(map(lambda x: x.cnt, word_ranks.values()))

    words = []
    for word_rank in sorted(word_ranks.values(), key=lambda x: x.cnt.rank + x.trend.rank)[:2000]:
        words.append(str(word_rank.word))

    return words

@util.time_logger
def build_post_cnt(db_dir):
    utc_now = datetime.utcnow()
    word_cnt = stats.get_word_cnt(db_dir)
    word_cnt_tuples = map(lambda x: (int(x), int(word_cnt[x])), word_cnt.keys())

    f_tmp = db_dir + "/word_cnt.db.tmp" 
    f = db_dir + "/word_cnt.db" 

    util.delete_if_exists(f_tmp)

    cur = stats.get_cursor(f_tmp)
    stats.create_given_tables(cur, ["chains_nouns", "post_cnt", "post_reply_cnt"])

    save_word_cnt(cur, word_cnt_tuples)
    words = get_trending_words(db_dir, word_cnt_tuples)

    mcur = stats.get_mysql_cursor(settings)
    count_currents2(cur, mcur, utc_now, words)

    os.rename(f_tmp, f)

def main():

    build_post_cnt(DB_DIR)



if __name__ == '__main__':
    main()
