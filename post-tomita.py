#!/usr/bin/python
import stats
import sys
import time
import os

db = os.environ["MOLVA_DB"]

def get_tweet_dates(cur):
    dates = cur.execute("""
        select substr(date, 0, 11) as date_ from tweets group by date_
    """).fetchall()

    ds = []
    for d in dates:
        d = d[0].replace("-", "_")
        ds.append(d)

    return ds

def main():
    cur = stats.get_cursor(db)
    
    dates = get_tweet_dates(cur)
    for d in dates:
        print "do for date " + d
        shard_db = db.replace(".db","_" + d + ".db")
        cur_shard = stats.get_cursor(shard_db)
        cur_shard.execute("""
            ATTACH "%s" as molva
        """ % db)
        stats.create_tables(cur_shard )
        stats.fill_tweet_chains(cur_shard, d)
        stats.fill_post_reply(cur_shard)

        cur_shard.execute("""
            DETACH molva
        """)

if __name__ == '__main__':
    main()
