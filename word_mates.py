#!/usr/bin/python
import sys
import os
import logging, logging.config
import json
from datetime import datetime, timedelta, date

import stats
from Indexer import Indexer
import util
from Fetcher import to_mysql_timestamp

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
def count_currents2(cur, mcur, utc_now, word_cnt_tuples):
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

    most_freq_words = []
    for i in sorted(word_cnt_tuples, key=lambda x: x[1], reverse=True)[:2000]:
        most_freq_words.append(str(i[0]))

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
        """ % (t, ",".join(most_freq_words), utc_ystd_tenminute)
        logging.debug(query)
        mcur.execute(query)

    mcur.execute("select word1, word2, cnt from word_mates_tmp")

    logging.info("Filling post_reply_cnt")
    cur.execute("begin transaction")
    while True:
        r = mcur.fetchone()        
        if r is None:
            break
        cur.execute("""
            insert into post_reply_cnt (post_md5, reply_md5, reply_cnt)
            values (%s, %s, %s)
        """ % r)
    cur.execute("commit")
    
    for t in ["post_cnt", "post_reply_cnt"]:
        _print_counts(cur, t) 

@util.time_logger
def count_currents(cur, utc_now):
    utc_ystd = (utc_now - timedelta(1)).strftime("%Y%m%d%H%M%S")
    utc_ystd_tenminute = utc_ystd[:11]

    cur.execute("drop table if exists word_mates_sum_raw")
    stats.create_given_tables(cur, {
        "word_mates_sum_raw": "word_mates_sum"
    })

    for db in ("today", "ystd"):
        cur.execute("""
            insert into word_mates_sum_raw
            select word1, word2, sum(cnt) 
            from %s.word_mates 
            where word1 in (
                select post_md5 
                from post_cnt
                order by post_cnt desc 
                limit 2000
            ) 
            and tenminute > %s
            group by word1, word2
        """ % (db, utc_ystd_tenminute))

    cur.execute("""
        insert into post_reply_cnt (post_md5, reply_md5, reply_cnt)
        select word1, word2, sum(cnt)
        from word_mates_sum_raw
        group by word1, word2
    """)
    
    for t in ["post_cnt", "post_reply_cnt"]:
        _print_counts(cur, t) 

@util.time_logger
def save_word_cnt(cur, word_cnt_tuples):
    cur.execute("begin transaction")
    cur.executemany("insert into post_cnt values (?, ?)", word_cnt_tuples)    
    cur.execute("commit")

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

    mcur = stats.get_mysql_cursor(settings)
    count_currents2(cur, mcur, utc_now, word_cnt_tuples)

    os.rename(f_tmp, f)

def main():

    build_post_cnt(DB_DIR)



if __name__ == '__main__':
    main()