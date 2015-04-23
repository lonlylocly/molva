#!/usr/bin/python
import sys
import os
import logging, logging.config
import json
from datetime import datetime, timedelta, date

import stats
from Indexer import Indexer
import molva.util as util
from Fetcher import to_mysql_timestamp

logging.config.fileConfig("logging.conf")

settings = {} 
try:
    settings = json.load(open('global-settings.json', 'r'))
except Exception as e:
    logging.warn(e)

DB_DIR = settings["db_dir"] if "db_dir" in settings else os.environ["MOLVA_DIR"]

@util.time_logger
def build_chains_nouns_all(cur, db, time_bound):
    logging.info("chains_nouns_all for db: %s" % db)
    cur.execute("""
        insert or ignore into chains_nouns_all 
        select tc.post_id, n1.noun_md5, tc.reply_id, n2.noun_md5, t.created_at 
        from %(db)s.tweet_chains tc 
        inner join %(db)s.tweets_nouns n1 
        on n1.id = tc.post_id 
        inner join %(db)s.tweets_nouns n2 
        on n2.id = tc.reply_id
        inner join %(db)s.tweets t 
        on tc.post_id = t.id
        where t.created_at > '%(time)s'
    """ % {"db": db, "time": time_bound})
    cur.execute("""
        insert or ignore into chains_nouns_all 
        select 
            tc.post_id, 
            n1.noun_md5, 
            tc.reply_id, 
            n2.noun_md5, 
            t.created_at 
        from %(db)s.tweet_chains tc 
        inner join %(db)s.tweets_nouns n1 
        on n1.id = tc.post_id 
        inner join %(db)s.tweets_nouns n2 
        on n2.id = tc.post_id
        inner join %(db)s.tweets t 
        on tc.post_id = t.id
        where 
            t.created_at > '%(time)s'
            and n1.noun_md5 != n2.noun_md5
    """ % {"db": db, "time": time_bound})

@util.time_logger
def _build_chains_nouns_init(cur, time_min, time_max):
    cur.execute("""
        insert into chains_nouns
        select p_id, p_md5, r_id, r_md5  from chains_nouns_all
        where created_at > '%(min_time)s' and created_at <= '%(max_time)s'
    """ % {"min_time": time_min, "max_time": time_max})

@util.time_logger
def _build_post_cnt(cur, suff, time_min, time_max):
    logging.info("post_cnt%s" % (suff))
    cmd = """
        insert or ignore into post_cnt%(suff)s 
        select p_md5,  count(*) 
        from (
            select p_id, p_md5 
            from chains_nouns_all
            where created_at > '%(min_time)s' and created_at <= '%(max_time)s' 
            group by p_md5, p_id
        ) group by p_md5
    """ % {"suff": suff, "min_time": time_min, "max_time": time_max}
    logging.info(cmd)
    cur.execute(cmd)

@util.time_logger
def build_chains_nouns(cur, time_ranges):
    logging.info("chains_nouns; time (%s, %s)" % (time_ranges[0]["min"], time_ranges[0]["max"]))
    _build_chains_nouns_init(cur, time_ranges[0]["min"], time_ranges[0]["max"])

@util.time_logger
def build_post_reply_cnt(cur):
    cur.execute("""
        insert or ignore into post_reply_cnt (post_md5, reply_md5, reply_cnt) 
        select p_md5, r_md5, count(*) 
        from chains_nouns 
        group by p_md5, r_md5;
    """)

@util.time_logger
def _delete_from_chains_nouns(cur, time_min):
    cur.execute("""
        delete from chains_nouns_all
        where created_at <= '%(time)s'
    """ % {'time': time_min})

def _print_counts(cur, t):
    cnt = cur.execute("select count(*) from %s" % t).fetchone()[0]
    logging.info("count(*) from %s = %s" % (t, cnt))

def attach_db(cur, db_file, db_name):
    query = "attach '%s' as %s" % (db_file, db_name)
    logging.info(query)
    cur.execute(query)

@util.time_logger
def count_currents(cur, utc_now):
    utc_ystd = utc_now - timedelta(1)
    utc_ystd_m = to_mysql_timestamp(utc_ystd) 

    time_ranges = [] 
    for i in (0, 1, 2, 3):
        time_ranges.append({})
        time_ranges[i]["min"] = to_mysql_timestamp(utc_now - timedelta(days = 1, hours=3 * i))
        time_ranges[i]["max"] = to_mysql_timestamp(utc_now - timedelta(hours=3 * i))
        time_ranges[i]["suff"] = "_n_" + str(i) if i != 0 else ""

    stats.create_given_tables(cur, ["chains_nouns_all"])

    for db in ("today", "ystd"):
        build_chains_nouns_all(cur, db, time_ranges[-1]["min"])

    build_chains_nouns(cur, time_ranges)

    build_post_reply_cnt(cur)
    
    for t in ["post_cnt", "post_reply_cnt", "chains_nouns_all"]:
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
    word_cnt_tuples = map(lambda x: (x, word_cnt[x]), word_cnt.keys())

    f_tmp = db_dir + "/word_cnt.db.tmp" 
    f = db_dir + "/word_cnt.db" 

    util.delete_if_exists(f_tmp)

    cur = stats.get_cursor(f_tmp)
    stats.create_given_tables(cur, ["chains_nouns", "post_cnt", "post_reply_cnt"])

    save_word_cnt(cur, word_cnt_tuples)

    yesterday = (utc_now - timedelta(1)).strftime("%Y%m%d")          
    today = (utc_now).strftime("%Y%m%d")    
    attach_db(cur, "%s/tweets_%s.db" % (db_dir, today), 'today')
    attach_db(cur, "%s/tweets_%s.db" % (db_dir, yesterday), 'ystd')

    count_currents(cur, utc_now)

    os.rename(f_tmp, f)

def main():

    build_post_cnt(DB_DIR)



if __name__ == '__main__':
    main()
