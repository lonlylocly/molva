# -*- coding: utf-8 -*-
import time
import os
import os.path
import re
import codecs
import logging, logging.config

import stats
import util

INDEX_CHUNK_SIZE = 50000

class IndexChunk:
    def __init__(self, date, chunk_id=None, index_file=None, done_file=None, facts_file=None):
        self.date = date
        self.chunk_id = chunk_id
        self.index_file = index_file
        self.done_file = done_file
        self.facts_file = facts_file

class Indexer:

    def __init__(self, db_dir):
        self.db_dir = db_dir
        self.index_dir = db_dir + "/index"
        self.nouns_dir = db_dir + "/nouns"
        self.dates_dbs = {}
        self.log = logging.getLogger('indexer')
        self.setup_dates_dbs()
        self.db_curs = {}

        try:
            os.makedirs(self.index_dir)
        except OSError as e:
            pass

    def _check_cursor_alive(self, cur):
        try:
            res = cur.execute("select 1").fetchone()
            if res[0] == 1:
                return True
            return False
        except Exception as e:
            logging.error(e)
            return False

    def get_db_for_filename(self, filename):
        if filename in self.db_curs and self._check_cursor_alive(self.db_curs[filename]):
            return self.db_curs[filename]
        else:
            self.log.info("Setup db connection  " + filename)
            cur = stats.get_cursor(filename)
            self.db_curs[filename] = cur

            return cur

    def get_db_for_date(self, date):
        if date in self.dates_dbs:
            return self.get_db_for_filename(self.dates_dbs[date])
        else:
            return None

    def setup_dates_dbs(self):
        dates_dbs = {}
        for f in os.listdir(self.db_dir):
            full_path = os.path.join(self.db_dir, f)
            if os.path.isfile(full_path):
                basename = os.path.basename(f)
                match = re.search("tweets_(\d{8})\.db$", basename)
                if match:
                    date = match.group(1)
                    dates_dbs[date] = os.path.join(self.db_dir, basename)

        self.dates_dbs = dates_dbs

    def _get_index_files(self):
        files = {} 
        for f in os.listdir(self.index_dir):
            full_path = os.path.join(self.index_dir, f)
            if os.path.isfile(full_path):
                match = re.search("^(\d{8})_(\d+)\.index\.txt", f)
                if match:
                    date, chunk_id = (match.group(1), match.group(2))
                    if date not in files:
                        files[date] = {}
                    files[date][chunk_id] = full_path
        return files

    def _get_fact_files(self):
        files = {} 
        for f in os.listdir(self.nouns_dir):
            full_path = os.path.join(self.nouns_dir, f)
            if os.path.isfile(full_path):
                match = re.search("^(\d{8})_(\d+)\.facts\.xml", f)
                if match:
                    date, chunk_id = (match.group(1), match.group(2))
                    if date not in files:
                        files[date] = {}
                    files[date][chunk_id] = full_path
        return files

    def get_nouns_to_parse(self):
        index_files = self._get_index_files()
        facts_files = self._get_fact_files()

        merged_files = {}
        for date in set(index_files.keys()) & set(facts_files.keys()):
            merged_files[date] = []
            chunk_ids = set(index_files[date].keys()) & set(facts_files[date].keys())
            for chunk_id in sorted(chunk_ids):
                merged_files[date].append((index_files[date][chunk_id], facts_files[date][chunk_id]))

        return merged_files

    def prepare_tweet_index_for_date(self, date, max_save_iter=60):
        util.try_several_times(lambda : self.add_new_tweets_for_tomita(date), 3)
        for i in range(0, max_save_iter):
            cnt = self.save_tweets_index(date)
            if cnt == 0:
                break

    def round_indexing(self):
        for date in sorted(self.dates_dbs.keys()):
            self.prepare_tweet_index_for_date(date)
                            

    def add_new_tweets_for_tomita(self, date):
        self.log.info("Index day %s" %date)
        cur = self.get_db_for_filename(self.dates_dbs[date])
        stats.create_given_tables(cur, ["tomita_progress", "tweets"])
    
        cur.execute("""
            INSERT OR IGNORE INTO tomita_progress (id)
            SELECT t.id from tweets t
            LEFT OUTER JOIN tomita_progress p
            ON t.id = p.id
            WHERE p.id is Null
        """)  

    def add_new_tweets_for_statuses(self, date):
        cur = self.get_db_for_filename(self.dates_dbs[date])
        stats.create_given_tables(cur, ["statuses_progress"])
    
        cur.execute("""
            INSERT OR IGNORE INTO statuses_progress (id)
            SELECT t.in_reply_to_id from tweets t
            LEFT OUTER JOIN statuses_progress p
            ON t.in_reply_to_id = p.id
            WHERE 
            t.in_reply_to_id is not Null 
            and p.id is Null
        """)  
       
    def create_index_files(self, date):
        cur_time = "%.0f" % time.time()
        file_prefix = os.path.join(self.index_dir, date + "_" + cur_time)
        self.log.info(file_prefix + ".index.txt")
        index_file = codecs.open(file_prefix + ".index.txt", "w", encoding="utf8")
        self.log.info(file_prefix + ".tweets.txt")
        tweets_file = codecs.open(file_prefix + ".tweets.txt", "w", encoding="utf8")

        return (index_file, tweets_file)

    def save_tweets_index(self, date):
        cur = self.get_db_for_filename(self.dates_dbs[date])

        index_file = None
        tweets_file = None
        
        tweets = cur.execute("""
            select p.id, t.tw_text, t.created_at 
            from tomita_progress p
            inner join tweets t
            on t.id = p.id
            where not p.id_done 
            order by p.id
            limit %s 
        """ % INDEX_CHUNK_SIZE)

        
        t = tweets.fetchone()
        if t is not None:
            index_file, tweets_file = self.create_index_files(date)

        last_tweet_id = None
        cnt = 0
        while t is not None:
            cnt += 1
            tweet_id, tw_text, created_at = t
            last_tweet_id = tweet_id
            index_file.write("%d\t%d\n" % (tweet_id, created_at))
            tw_text = tw_text.replace('\n', ' ').replace("'", "\\'")
            # filter out usernames
            tw_text = re.sub("(^@|\s@)[^\s]+", "", tw_text)
            tweets_file.write("%s\n" % tw_text)
            t = tweets.fetchone()

        for f in (index_file, tweets_file):
            if f is not None:
                f.close()

        self.log.info("Dumped %s ids" % cnt)

        if last_tweet_id is not None:
            f = lambda : cur.execute("""
                update tomita_progress
                set id_done = 1
                where id <= ?
            """, (last_tweet_id, ))
            util.try_several_times(f, 3)

        return cnt
 
