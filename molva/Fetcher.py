# -*- coding: utf-8 -*-
import logging
from datetime import datetime, timedelta
from datetime import time as datetimeTime
import json
import re
from datetime import date
import traceback

import molva.stats as stats
from molva.TwitterClient import TwitterClient
from molva.Exceptions import WoapeException
import molva.util as util

MYSQL_TIMESTAMP = "%Y%m%d%H%M%S"

RETRY_TIMEOUT = 60

def get_tw_create_time(t):
    create_time = t["created_at"]
    create_time = re.sub("[+-]\d\d\d\d", "", create_time)
    dt = datetime.strptime(create_time, "%a %b %d %H:%M:%S  %Y")
   
    return dt

def from_sqlite_timestamp(mysql_time):
    return datetime.strptime(mysql_time,"%Y-%m-%d %H:%M:%S")

def to_mysql_timestamp(dt):
    return dt.strftime(MYSQL_TIMESTAMP)


class Fetcher:
    
    def __init__(self, db_dir, headers,  days_back=7, seconds_till_user_retry=3600):
        db_basename="tweets"
        self.db_dir = db_dir
        self.db_basename = db_basename
        self.dates_db = {}
        self.days_back = days_back
        self.recent_users = {}
        self.seconds_till_user_retry = seconds_till_user_retry
        self.log = logging.getLogger('fetcher-' + db_basename)

        cur = stats.get_main_cursor(self.db_dir)
        self.main_db = cur 
        stats.create_given_tables(cur, ["users"])
        
        self.client = TwitterClient(headers)
    
    def get_db_for_date(self, date):
        date = date[:8] # assume date format %Y%m%d_%H%M%S

        if date in self.dates_db:
            return self.dates_db[date]
        else:
            self.log.info("Setup db connection for date " + date)
            cur = stats.get_cursor(self.db_dir + "/tweets_" + date + ".db")
            self.dates_db[date] = cur
            stats.create_given_tables(cur, ["tweets"])

            return cur

    def get_tweet_start_time(self):
        # date-time border glued to start of day
        return datetime.combine(datetime.now(), datetimeTime(0, 0, 0)) - timedelta (days = self.days_back)

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

    def mark_user_done(self, cur, username, max_id=0, auth_failed = False):
        logging.info( "%s" % username)
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


    def get_more(self, cur, username, max_id=None, since_id=None):
        resp = self.client.get_more(cur, username, max_id, since_id)

        if resp.status == 429:
            logging.warn("Limit exceeded; waiting 60 sec")
            logging,debug( json.dumps(resp.getheaders(), indent=4))

            time.sleep(RETRY_TIMEOUT)

            raise WoapeException("Rate limit exceeded")

        if resp.status is not 200:
            logging.error("Response code: %s %s. Response body: %s"% (resp.status, resp.reason, resp.read() ))
            self.mark_user_done(cur, username, auth_failed = (resp.status == 401))
            raise WoapeException('Invalid response')        

        return resp

    def save_tweet(self, reply):
        try:
            if "text" not in reply:
                return

            mysql_time = to_mysql_timestamp(get_tw_create_time(reply))
            cur = self.get_db_for_date(mysql_time)
            cur.execute("""
                INSERT OR IGNORE INTO tweets
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (reply["id"], reply["text"], reply["user"]["screen_name"], reply["user"]["id"], 
                reply["in_reply_to_screen_name"], reply["in_reply_to_status_id"], mysql_time))
        except Exception as e:
            traceback.print_exc()
            self.log.error(e)

    def iteration(self):
        username, since_id = self.get_next_user()
        self.update_recent_users()

        oldest_tweet_time = datetime.now()
        next_fetch_max_id = None
        max_id = since_id 
        cnt = 0

        while oldest_tweet_time > self.get_tweet_start_time():
            resp = self.get_more(self.main_db, username, next_fetch_max_id, since_id) 

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
        self.mark_user_done(self.main_db, username, max_id)

        self.log.info("Oldest tweet time: %s " % (oldest_tweet_time))
        
        self.log.info("Saved %s tweets" % cnt)

    def lookup_statuses_iteration(self, cur, ids):
        ids_enc = ",".join(map(str, ids))
        resp = self.client.post_path('/1.1/statuses/lookup.json', {'id': ids_enc})

        if resp.status is not 200:
            loggig.error(u"Response code: %s %s. Response body: %s"% (resp.status, resp.reason, resp.read() ))

            raise WoapeException('Invalid response')        

        content = resp.read()

        cont = json.loads(content)
        saved = 0
        too_old = 0
        for t in cont:
            if not util.got_russian_letters(t["text"]):
                continue

            mysql_time = to_mysql_timestamp(get_tw_create_time(t))
            yesterday = (date.today() - timedelta(1)).strftime("%Y%m%d")
            if mysql_time[:8] >= yesterday:
                self.save_tweet(t)
                saved += 1
            else:
                too_old += 1

        logging.info("Saved %s statuses" % saved)
        logging.info("Not saved %s statuses (too old)" % too_old)

        cur.execute("update statuses_progress set id_done = 1 where id in (%s) " % ids_enc)


