#!/usr/bin/python
import httplib, urllib
import base64
import json
import sqlite3
import re
import time
from sets import Set
import sys,codecs
from datetime import datetime, timedelta
from datetime import time as datetimeTime
import os
import traceback
import copy
import logging, logging.config

import util
import stats

logging.config.fileConfig("logging.conf")

sys.stdout = codecs.getwriter('utf8')(sys.stdout)

headers = {"Authorization":"Bearer AAAAAAAAAAAAAAAAAAAAAEBgVAAAAAAAxZcUIQhxg"+
"3gWnlJBMHUJ%2FSYIwbc%3DuIu1L6qBROC2wnONa37BVjw0z35FbjJSL2XuXa8fuCUc8wAWJW"}

#c.request('GET', '/1.1/search/tweets.json?%s'%params, '', headers)
#c.request('GET', '/1.1/statuses/show.json?id=%s'%tweet_id, '', headers)

CHAINS_GOAL = 10000000
#TWEETS_START_DAY = datetime(20114, 12, 25, 0, 0, 0) #datetime.now() - timedelta (days = 40)
#TWEETS_START_DAY = datetime.now() - timedelta (days = 2)
DB_DIR = os.environ["MOLVA_DIR"] 


class WoapeException(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

def create_tables(cur):
    stats.create_given_tables(cur, ["tweets", "users"])

DEFAULT_RATE_LIMIT_SLEEP = 60

cur_limit = DEFAULT_RATE_LIMIT_SLEEP

def get_path(path):
    c = httplib.HTTPSConnection('api.twitter.com')
    c.set_debuglevel(0)
    print "[%s] Open path: %s" % (time.ctime(), path)
    c.request('GET', path, '', headers)
    resp = c.getresponse()

    if resp.status == 429:
        print "Limit exceeded; waiting 60 sec" 
        print json.dumps(resp.getheaders(), indent=4)
        time.sleep(cur_limit)

    return resp
 
def post_path(path, params):
    c = httplib.HTTPSConnection('api.twitter.com')
    c.set_debuglevel(0)
    params_encoded = urllib.urlencode(params)
    headers_post = copy.deepcopy(headers)
    headers_post["Content-type"] = "application/x-www-form-urlencoded"
    headers_post["Accept"] = "text/plain"

    print "[%s] Open path: %s, body: %s" % (time.ctime(), path, params_encoded)
    resp = c.request("POST", path, params_encoded, headers_post)
    return c.getresponse()


def get_tweet_text(tweet_id) :
    path = '/1.1/statuses/show.json?id=%s'%tweet_id
    resp = get_path(path)
    if resp is not None:
        s = json.loads(resp.read())
        if "text" in s:
            return s["text"]
    return None

def fetch_list(cur, query):
    return map(lambda x: x[0], cur.execute(query).fetchall())

def get_chains(cur):
    ids = fetch_list(cur, "select id from tweets")
    in_reply_to_ids = fetch_list(cur, "select in_reply_to_id from tweets")
    s1 = Set(ids)
    s2 = Set(in_reply_to_ids)
    chains = s1 & s2    
    return chains

def mark_user_done(cur, username, max_id=0, auth_failed = False):
    print "%s" % username
    blocked_user = 1 if auth_failed else 0
    cur.execute("""
        update users 
        set 
            reply_cnt = 0 , 
            since_id = ?, 
            blocked_user = ?, 
            done_time = datetime('now') 
        where username = ?
    """, (max_id, blocked_user, username ))

def try_several_times(f, times, error_return=[]):
    tries = 0
    while tries < times:
        try:
            tries += 1
            res = f()
            return res
        except WoapeException as e:
            print "[%s] Stop trying, WoapeException: %s" % (time.ctime(), e)
            break
        #except Exception as e:
        #    traceback.print_exc()
        #    print "[%s] [ERROR] %s" % (time.ctime(), e)

    return error_return

def get_tw_create_time(t):
    create_time = t["created_at"]
    create_time = re.sub("[+-]\d\d\d\d", "", create_time)
    dt = datetime.strptime(create_time, "%a %b %d %H:%M:%S  %Y")
   
    return dt

MYSQL_TIMESTAMP = "%Y%m%d_%H%M%S"
def from_sqlite_timestamp(mysql_time):
    return datetime.strptime(mysql_time,"%Y-%m-%d %H:%M:%S")

def to_mysql_timestamp(dt):
    return dt.strftime(MYSQL_TIMESTAMP)

def get_more(cur, username, max_id=None, since_id=None):
    resp = None
    max_id_str = "" if max_id is None else "&max_id=" + str(max_id)
    since_id_str = "" if since_id is None  or since_id == 0 else "&since_id=" + str(since_id)

    resp = get_path('/1.1/statuses/user_timeline.json?screen_name=%s&count=200%s%s' % (username,max_id_str, since_id_str))

    if resp.status == 429:
        raise WoapeException("Rate limit exceeded")

    if resp.status is not 200:
        print "[ERROR] Response code: %s %s. Response body: %s"% (resp.status, resp.reason, resp.read() )
        mark_user_done(cur, username, auth_failed = (resp.status == 401))
        raise WoapeException('Invalid response')        

    return resp

class Fetcher:
    
    def __init__(self, db_dir, db_basename="tweets", days_back=7, seconds_till_user_retry=3600):
        self.db_dir = db_dir
        self.db_basename = db_basename
        self.dates_db = {}
        self.days_back = days_back
        self.recent_users = {}
        self.seconds_till_user_retry = seconds_till_user_retry
        self.log = logging.getLogger('fetcher-' + db_basename)

        cur = stats.get_cursor(self.db_dir + "/" + self.db_basename + ".db")
        self.main_db = cur 
        stats.create_given_tables(cur, ["users"])
        
    def get_db_for_date(self, date):
        date = date[:8] # assume date format %Y%m%d_%H%M%S

        if date in self.dates_db:
            return self.dates_db[date]
        else:
            self.log.info("Setup db connection for date " + date)
            cur = stats.get_cursor(self.db_dir + "/" + self.db_basename + "_" + date + ".db")
            self.dates_db[date] = cur
            stats.create_given_tables(cur, ["tweets"])

            return cur

    def get_tweet_start_time(self):
        # date-time border glued to start of day
        return datetime.combine(datetime.now(), datetimeTime(0, 0, 0)) - timedelta (days = self.days_back)

    def iteration_handler(self):
        f = lambda :  self.iteration()
        try_several_times(f, 3, [])

    def get_next_user(self):     
        username, since_id = self.main_db.execute("""
            SELECT username, since_id
            FROM users 
            WHERE
                (
                    done_time = '' 
                OR 
                    (strftime('%s','now') - strftime('%s', datetime(done_time))) > ? 
                )
            AND
                not blocked_user
            ORDER BY reply_cnt desc, since_id asc
            LIMIT 1
        """, (self.seconds_till_user_retry,)).fetchone()

        if username is None:
            raise WoapeException('Cannot get next username')
    
        return (username, since_id)

    def update_recent_users(self):
        users_rows = self.main_db.execute("""
            SELECT username, done_time
            FROM users
            WHERE since_id != 0 and done_time != ''
        """).fetchall()

        users = {}

        for row in users_rows:
            username, done_time = row
            users[username] = from_sqlite_timestamp(done_time)

        self.recent_users = users

    def iteration(self):
        username, since_id = self.get_next_user()
        self.update_recent_users()

        oldest_tweet_time = datetime.now()
        next_fetch_max_id = None
        max_id = since_id 
        cnt = 0

        while oldest_tweet_time > self.get_tweet_start_time():
            resp = get_more(self.main_db, username, next_fetch_max_id, since_id) 

            content = resp.read()
            last_max_id = next_fetch_max_id

            conv_partners = {}
            for t in json.loads(content):
                ct = get_tw_create_time(t) 
                if ct < oldest_tweet_time:
                    oldest_tweet_time = ct
                if ct < self.get_tweet_start_time():
                    oldest_tweet_time = ct
                    break
                if not util.got_russian_letters(t["text"]):
                    continue
                if next_fetch_max_id is None or next_fetch_max_id > int(t["id"]):
                    next_fetch_max_id = int(t["id"])
                if int(t["id"]) > max_id:
                    max_id = int(t["id"]) 

                self.save_tweet(t)
                cnt = cnt + 1

                fellow = t["in_reply_to_screen_name"]
                if fellow is None or fellow == "":
                    continue
                if fellow not in conv_partners:
                    conv_partners[fellow] = 0
                # count mention to recently done user if it's newer than fellow.done_time
                if fellow in self.recent_users and self.recent_users[fellow] is not None :
                    if self.recent_users[fellow] < ct:
                        conv_partners[fellow] = conv_partners[fellow] + 1
                # count mention if fellow never seen before 
                else:
                    conv_partners[fellow] = conv_partners[fellow] + 1

            self.main_db.executemany("insert or ignore into users (username) values (?)", map(lambda x: (x, ), conv_partners.keys()))

            for partner in conv_partners:
                self.main_db.execute("update users set reply_cnt = reply_cnt + ? where username = ?", (conv_partners[partner], partner))

            if next_fetch_max_id is None or next_fetch_max_id == last_max_id:
                self.log.info("cannot get valid max_id")
                break    
        mark_user_done(self.main_db, username, max_id)

        self.log.info("Oldest tweet time: %s " % (oldest_tweet_time))
        
        self.log.info("Saved %s tweets" % cnt)

    def save_tweet(self, reply):
        try:
            mysql_time = to_mysql_timestamp(get_tw_create_time(reply))
            cur = self.get_db_for_date(mysql_time)
            cur.execute("""
                INSERT OR IGNORE INTO tweets
                VALUES (?, ?, ?, ?, ?, ?)
            """, (reply["id"], reply["text"], reply["user"]["screen_name"], reply["in_reply_to_screen_name"], reply["in_reply_to_status_id"], mysql_time))
        except Exception as e:
            traceback.print_exc()
            self.log.error(e)

def main():
    start_users = sys.argv[1:]

    fetcher = Fetcher(DB_DIR)

    cur = fetcher.main_db

    for user in start_users:
        cur.execute("replace into users values ('%s', 0, 0) " % user)

    while True:
        fetcher.iteration_handler()
    
if __name__ == "__main__":
    main()
