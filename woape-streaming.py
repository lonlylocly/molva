#!/usr/bin/python
# -*- coding: utf-8 -*-
import tweepy

import sys
import os
import logging, logging.config
import json
import re
import traceback
from datetime import datetime, timedelta
from datetime import time as datetimeTime

import stats
from Indexer import Indexer
import util
from Fetcher import Fetcher

logging.config.fileConfig("logging.conf")

settings = {} 
try:
    settings = json.load(open('streaming-settings.json', 'r'))
except Exception as e:
    logging.warn(e)

DB_DIR = settings["db_dir"] if "db_dir" in settings else os.environ["MOLVA_DIR"]

#def save_tweet(reply):
#    logging.info("Save tweet: %s" % reply["text"])
#    try:
#        mysql_time = to_mysql_timestamp(get_tw_create_time(reply))
#        cur.execute("""
#            INSERT OR IGNORE INTO tweets
#            VALUES (?, ?, ?, ?, ?, ?)
#        """, (reply["id"], reply["text"], reply["user"]["screen_name"], reply["in_reply_to_screen_name"], reply["in_reply_to_status_id"], mysql_time))
#    except Exception as e:
#        traceback.print_exc()
#        logging.error(e)

fetcher = Fetcher(DB_DIR, {})

NOTIFY_PERIOD = 50

class RussianMentionsListener(tweepy.StreamListener):

    def __init__(self):
        self.queue_cnt = 0
    
    def on_data(self, data):
        t = {}
        try:
            t = json.loads(data)

            if "text" not in t:
                return
                                
            if not util.got_russian_letters(t["text"]):
                return

            #if t["in_reply_to_status_id"] is None:
            #    return 
            
            fetcher.save_tweet(t) 
            self.queue_cnt += 1
            if self.queue_cnt == NOTIFY_PERIOD:
                logging.info("Got %s tweets" % (NOTIFY_PERIOD))
                self.queue_cnt = 0

            return
        except Exception as e:
            logging.error(e)
            traceback.print_exc()
            print t
            return

    def on_error(self, status):
        print status

if __name__ == '__main__':
    listener = RussianMentionsListener()
    auth = tweepy.OAuthHandler(settings["consumer_key"], settings["consumer_secret"])
    auth.set_access_token(settings["access_token"], settings["access_token_secret"])


    stream = tweepy.Stream(auth, listener)
    stream.filter(track=settings["russian_keywords"])
    #stream.sample()
