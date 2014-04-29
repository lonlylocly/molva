#!/usr/bin/python
import os
import re
import sqlite3
from datetime import datetime, timedelta

import stats


def get_tw_create_time(create_time):
    create_time = re.sub("[+-]\d\d\d\d", "", create_time)
    dt = datetime.strptime(create_time, "%a %b %d %H:%M:%S  %Y")
    return dt


def main():
    con = sqlite3.connect(os.environ["MOLVA_DB"])
 
    cur = con.cursor()
    
    cur.execute("""
        create table if not exists tweet_date
        (
            id integer,
            date text,
            primary key (id)
        )
    """)

    res = cur.execute("""
        select id, created_at from tweets
    """).fetchall()

    print "fetched all"

    cnt = 0
    buf = []
    for i in res:
        cnt += 1 
        _id, _time = i
        _time = get_tw_create_time(_time)
        buf.append([_id, _time])
        if cnt > 100000:
            print "iter done"
            cnt = 0
            cur.executemany("insert into tweet_date values (?, ?)", buf)
            buf = []
        
    cur.executemany("insert into tweet_date values (?, ?)", buf)
   
    con.commit() 
    
if __name__ == "__main__":
    main()


