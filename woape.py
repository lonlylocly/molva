#!/usr/bin/python
import base64
import json
import sqlite3
import time
import sys,codecs
from datetime import datetime, timedelta
from datetime import time as datetimeTime
import os
import traceback
import logging, logging.config

import util
from util import try_several_times
import stats
from Fetcher import Fetcher
from Exceptions import WoapeException


logging.config.fileConfig("logging.conf")

sys.stdout = codecs.getwriter('utf8')(sys.stdout)


settings = {} 
try:
    settings = json.load(open('global-settings.json', 'r'))
except Exception as e:
    logging.warn(e)

DB_DIR = settings["db_dir"] if "db_dir" in settings else os.environ["MOLVA_DIR"]

headers = settings["headers"]
 
def create_tables(cur):
    stats.create_given_tables(cur, ["tweets", "users"])

#def get_tweet_text(tweet_id) :
#    path = '/1.1/statuses/show.json?id=%s'%tweet_id
#    resp = get_path(path)
#    if resp is not None:
#        s = json.loads(resp.read())
#        if "text" in s:
#            return s["text"]
#    return None

def fetch_list(cur, query):
    return map(lambda x: x[0], cur.execute(query).fetchall())

def get_chains(cur):
    ids = fetch_list(cur, "select id from tweets")
    in_reply_to_ids = fetch_list(cur, "select in_reply_to_id from tweets")
    s1 = set(ids)
    s2 = set(in_reply_to_ids)
    chains = s1 & s2    
    return chains

def main():
    start_users = sys.argv[1:]

    fetcher = Fetcher(DB_DIR, headers)

    cur = fetcher.main_db

    for user in start_users:
        cur.execute("replace into users values ('%s', 0, 0) " % user)

    while True:
        f = lambda :  fetcher.iteration()
        try_several_times(f, 3, [])

if __name__ == "__main__":
    main()
