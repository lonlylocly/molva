#!/usr/bin/python
# -*- coding: UTF-8 -*-
import tornado.ioloop
import tornado.web
import sys
import os
import json
import time
import re
import logging
from subprocess import check_output
import sqlite3
from datetime import datetime, timedelta
from dateutil import tz

import molva.stats as stats
from molva.Indexer import Indexer
import molva.util as util

logging.config.fileConfig("logging.conf")

settings = json.load(open('global-settings.json', 'r'))

class ClusterHandler(tornado.web.RequestHandler):

    def get_clusters(self, skip, before, date):
        cur = stats.get_cursor(settings["db_dir"] + "/tweets_display.db") 
        if date is not None:
            cur.execute("""
                select cluster 
                from clusters 
                where cluster_date = '%(date)s'
            """  % ({'date': date}))
        elif before is not None:
            cur.execute("""
                select cluster 
                from clusters 
                where cluster_date < '%(before)s'
                order by cluster_date desc 
                limit 1 
            """ % ({'before': before}))
        else:
            cur.execute("""
                select cluster 
                from clusters 
                order by cluster_date desc 
                limit 1 
                offset %s
            """ % (skip))
        res = cur.fetchone()[0] 

        return res

    def parse_date(self, mydate):
        if mydate is None or mydate == "":
            return None
        try:
            mydate = mydate.replace("-","").replace(" ","").replace(":","") 

            unixtime = datetime.strptime(mydate, "%Y%m%d%H%M%S").strftime("%s")
            mydate_dt = datetime.utcfromtimestamp(int(unixtime))
            mydate = mydate_dt.strftime("%Y%m%d%H%M%S")
            
            return mydate
        except Exception as e:
            logging.info(e)
            return None

    def get(self):
        skip = self.get_argument("skip", default=0)
        before = self.parse_date(self.get_argument("before", default=None))
        date = self.parse_date(self.get_argument("date", default=None))
        try:
            skip = int(skip)
        except:
            skip = 0

        logging.info("Before %s (UTC)" % before)
        logging.info("Date %s (UTC)" % before)

        cl = self.get_clusters(skip, before, date)

        self.write(cl)

class RelevantHandler(tornado.web.RequestHandler):

    @util.time_logger
    def get_relevant(self, date):
        cur = stats.get_cursor(settings["db_dir"] + "/tweets_relevant.db") 
        if date is not None:
            cur.execute("""
                select relevant 
                from relevant
                where cluster_date = '%(date)s'
            """  % ({'date': date}))

        res = cur.fetchone()[0] 

        return res

    def parse_date(self, mydate):
        if mydate is None or mydate == "":
            return None
        try:
            mydate = mydate.replace("-","").replace(" ","").replace(":","") 

            unixtime = datetime.strptime(mydate, "%Y%m%d%H%M%S").strftime("%s")
            mydate_dt = datetime.utcfromtimestamp(int(unixtime))
            mydate = mydate_dt.strftime("%Y%m%d%H%M%S")
            
            return mydate
        except Exception as e:
            logging.info(e)
            return None

    def get(self):
        date = self.parse_date(self.get_argument("date", default=None))
        
        logging.info("Date %s (UTC)" % date)

        r = self.get_relevant(date)

        logging.info("Done fetch")

        self.write(r)

def utc_to_local(utctime):
    from_zone = tz.tzutc()
    to_zone = tz.tzlocal()

    utc = datetime.strptime(utctime, '%Y%m%d%H%M%S')

    utc = utc.replace(tzinfo=from_zone)

    local = utc.astimezone(to_zone)

    local_str = local.strftime("%Y%m%d%H%M%S")

    return local_str

class TrendHandler(tornado.web.RequestHandler):

    @util.time_logger
    def get_word_time_cnt(self, word_md5, time1, time2):
        logging.info("Get word time cnt: %s, %s, %s" % (word_md5, time1, time2))
        utc_now = datetime.utcnow()
        res = []
        default_left_time_bound = (utc_now - timedelta(3)).strftime("%Y%m%d%H%M%S")[:10]
        time = ""
        if time1 is not None:
            time += " and hour >= " + str(time1)[:10]
        else:
            time += " and hour >= " + default_left_time_bound
        if time2 is not None:
            time += " and hour < " + str(time2)[:10]

        where = "word_md5 = %s" % word_md5
        if word_md5 == util.digest('0'):
            where = "1"

        mcur = stats.get_mysql_cursor(settings)
        try:
            for day in [3, 2, 1, 0]:
                date = (utc_now - timedelta(day)).strftime("%Y%m%d")
                #stats.create_mysql_tables(mcur, {"word_hour_cnt_"+date: "word_hour_cnt"})
                mcur.execute("""
                    SELECT word_md5, hour, cnt
                    FROM word_hour_cnt_%(date)s
                    WHERE %(where)s 
                    %(time)s
                """ % {"where": where, "time": time, "date": date})
                while True:
                    r = mcur.fetchone()
                    if r is None:
                        break
                    word, hour, cnt = r
                    utctime = str(hour) + "0000"
                    utc_unixtime = datetime.strptime(utctime, '%Y%m%d%H%M%S').strftime('%s')
                    res.append((str(word), utc_to_local(utctime), int(cnt), utc_unixtime))
            logging.info("word time cnt: %s" % len(res))
        except Exception as e:
            logging.error(e)

        return res

    def parse_times(self, time1, time2):
        try:
            if time1 is not None:
                time1 = "{:0<11d}".format(int(time1))
            if time2 is not None:
                time2 = "{:0<11d}".format(int(time2))
            if time1 is not None and time2 is not None and time1 > time2:
                time1, time2 = time2, time1
        except Exception as e:
            logging.error(e)
            time1 = None
            time2 = None

        return (time1, time2)

    @util.time_logger
    def get(self):
        try:
            word = self.get_argument("word", default=None)
            time1 = self.get_argument("time1", default=None)
            time2 = self.get_argument("time2", default=None)
            logging.info("Request: %s, %s, %s" % (word, time1, time2))

            if word is None:
                return
            
            time1, time2 = self.parse_times(time1, time2)

            word_md5 = util.digest(word.strip())       
            logging.info("Get time series for '%s' (%s)" % (word, word_md5))
           
            res = self.get_word_time_cnt(word_md5, time1, time2)

            res = sorted(res, key=lambda x: x[1])
            res = map(lambda x: {"hour": x[1], "count": x[2], "utc_unixtime": x[3]}, res)
            #mov_av = [0]
            #for i in range(1, len(res) -1):
            #    ma = float(res[i-1]["count"] + res[i]["count"] + res[i+1]["count"]) / 3
            #    mov_av.append(ma)
            #mov_av.append(0)

            self.write(json.dumps({"word": word_md5, "dataSeries": res}))
        except Exception as e:
            logging.error(e)
            raise e
   
class QualityMarkHandler(tornado.web.RequestHandler):

    def post(self):
        req_data = None
        try:
            req_data = json.loads(self.request.body)

            if req_data is not None:
                cur = stats.get_cursor(settings["db_dir"] + "/quality_marks.db") 
                stats.create_given_tables(cur, ["quality_marks"])
                username = ""
                if "username" in req_data and req_data["username"] is not None:
                    username = req_data["username"]
                update_time = ""
                if "update_time" in req_data and req_data["update_time"] is not None:
                    update_time = req_data["update_time"]
                    update_time = int(re.sub('[-\s:]','', update_time))
                exp_name = ""
                if "experiment_name" in req_data and req_data["experiment_name"] is not None:
                    exp_name = req_data["experiment_name"]
                exp_descr = ""
                if "experiment_descr" in req_data and req_data["experiment_descr"] is not None:
                    exp_descr = req_data["experiment_descr"]

                cur.execute("""
                    insert into quality_marks 
                    (update_time, username, exp_name, exp_descr,  marks) 
                    values (?, ?, ?, ?, ?)
                """, (update_time, username, exp_name, exp_descr, json.dumps(req_data["marks"])))

        except Exception as e:
            logging.error(e)
            raise(e)

        self.write("")

if __name__ == '__main__':
    application = tornado.web.Application([
        (r"/api/cluster", ClusterHandler),
        (r"/api/relevant", RelevantHandler),
        (r"/api/trend", TrendHandler),
        (r"/api/mark_topic", QualityMarkHandler)
    ])
    application.listen(8000)
    tornado.ioloop.IOLoop.instance().start()
