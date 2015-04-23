#!/usr/bin/python
# -*- coding: utf-8 -*-
import hashlib
import re
import argparse
import traceback
import logging
import time
import os
import os.path
from datetime import datetime, timedelta, date

from molva.Exceptions import WoapeException

def digest_large(s):
    large = int(hashlib.md5(s.encode('utf-8')).hexdigest(), 16)

    return large

def digest(s):
    large = int(hashlib.md5(s.encode('utf-8')).hexdigest(), 16)

    b1 = large & (2 ** 32 - 1)
    b2 = large >> 32 & (2 ** 32 - 1)
    b3 = large >> 64 & (2 ** 32 - 1)
    b4 = large >> 96 & (2 ** 32 - 1)
    small = b1 ^ b2 ^ b3 ^ b4

    return small

def got_russian_letters(s, k=3):
    # has k or more russian letters
    res = re.match(u".*([а-яА-Я])+.*" , s) is not None
    return res

def get_dates_range_parser():
    parser = argparse.ArgumentParser()

    parser.add_argument("-s", "--start")
    parser.add_argument("-e", "--end")

    return parser

def try_several_times(f, times, finilizer=None):
    tries = 0
    while tries < times:
        try:
            tries += 1
            logging.info("Starting try #%s" % tries)
            res = f()
            return res
        except Exception as e:
            traceback.print_exc()
            logging.error(e)
            if finilizer is not None:
                finilizer()

    raise FailedSeveralTimesException("")

def time_logger(func):
    def inner(*args, **kwargs):
        inner.__name__ = func.__name__
        logging.info("Starting <%s>" % func.__name__)
        start_time =time.time()
        try:
            res = func(*args, **kwargs) 
        finally:
            end_time=time.time()
            logging.info("<%s> Time spent: %.3f seconds" % (func.__name__, end_time - start_time))

        return res

    return inner

def delete_if_exists(f):
    if os.path.exists(f):
        os.remove(f)

def get_recent_days(utc_now = datetime.utcnow(), days=2):
    dates = []
    for i in range(0,days):
        d = (utc_now - timedelta(i)).strftime("%Y%m%d")          
        dates.append(d)

    return dates

def get_yesterday_tenminute(utc_now = datetime.utcnow(), days=1):
    utc_ystd = (utc_now - timedelta(days)).strftime("%Y%m%d%H%M%S")
    utc_ystd_tenminute = utc_ystd[:11]

    return utc_ystd_tenminute

def filter_trash_words_cluster(clusters):
    filtered_cl = []
    total_md5 = digest("__total__")
    for c in clusters:
        filter_cluster = False
        for m in c["members"]:
            if m["id"] == total_md5:
                filter_cluster = True
                break
        if not filter_cluster:
            filtered_cl.append(c)
        else:
            logging.info("Filtered cluster containing __total__ with %s elements" % len(c))

    return filtered_cl

