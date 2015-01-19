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
def build_tweets_nouns_cur(cur, db, time_bound):
    logging.info("tweets_nouns_cur for db: %s" % db)
    cur.execute("""
        insert into tweets_nouns_cur
        select n.id, n.noun_md5 from %(db)s.tweets_nouns n
        inner join %(db)s.tweets t
        on n.id = t.id
        where t.created_at > '%(time)s'
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

    for i in range(0, len(time_ranges)):
        suff = time_ranges[i]["suff"]
        _build_post_cnt(cur, suff, time_ranges[i]["min"], time_ranges[i]["max"])

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

    cur_tables = ["chains_nouns", "post_cnt", "post_reply_cnt"]
    for t in cur_tables:
        logging.info("drop %s" % t)
        cur.execute("drop table if exists %s" % t)
    stats.create_given_tables(cur, cur_tables)
    stats.create_given_tables(cur, ["chains_nouns_all"])

    cur_tables2 = {
        "tweets_nouns_cur": "tweets_nouns", 
        "post_cnt_n_1": "post_cnt",
        "post_cnt_n_2": "post_cnt",
        "post_cnt_n_3": "post_cnt",
    }
    for t in cur_tables2:
        logging.info("drop %s" % t)
        cur.execute("drop table if exists %s" % t)
    stats.create_given_tables(cur, cur_tables2)

    _delete_from_chains_nouns(cur,  time_ranges[-1]["min"])

    for db in ("today", "ystd"):
        build_chains_nouns_all(cur, db, time_ranges[-1]["min"])

        build_tweets_nouns_cur(cur, db, utc_ystd_m)

    build_chains_nouns(cur, time_ranges)

    build_post_reply_cnt(cur)
    
    for t in cur_tables + cur_tables2.keys():
        _print_counts(cur, t) 

def main():

    utc_now = datetime.utcnow()
    yesterday = (utc_now - timedelta(1)).strftime("%Y%m%d")          
    today = (utc_now).strftime("%Y%m%d")

    ind = Indexer(DB_DIR)
    cur = stats.get_main_cursor(DB_DIR)

    today_file = ind.dates_dbs[today]
    ystd_file = ind.dates_dbs[yesterday]

    logging.info("Attach today: %s" % today_file)
    cur.execute("attach '%s' as today" % today_file)

    logging.info("Attach ystd: %s" % ystd_file)
    cur.execute("attach '%s' as ystd" % ystd_file)

    count_currents(cur, utc_now)


if __name__ == '__main__':
    main()
