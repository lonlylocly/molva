#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys
import os
import logging, logging.config
import json

import stats
from Indexer import Indexer
import molva.util as util

logging.config.fileConfig("logging.conf")

settings = {} 
try:
    settings = json.load(open('global-settings.json', 'r'))
except Exception as e:
    logging.warn(e)

POST_MIN_FREQ = settings["post_min_freq"] if "post_min_freq" in settings else 10

DB_DIR = settings["db_dir"] if "db_dir" in settings else os.environ["MOLVA_DIR"]

BLOCKED_NOUNS_LIST = u"\n".join(list(u"абвгдеёжзиклмнопрстуфхцчшщыьъэюя"))

BLOCKED_NOUNS = ",".join(map( lambda x: str(util.digest(x)), BLOCKED_NOUNS_LIST.split("\n")))

NOUNS_LIMIT = 2000

def main():
    logging.info("start")
    parser = util.get_dates_range_parser()
    parser.add_argument("-c", "--clear", action="store_true")
    parser.add_argument("-p", "--profiles-table", default="post_reply_cnt")
    parser.add_argument("-o", "--out-file")
    args = parser.parse_args()

    cur = stats.get_cursor(DB_DIR + "/word_cnt.db")
            
    profiles_dict = stats.setup_noun_profiles(cur, {}, {}, 
        post_min_freq = POST_MIN_FREQ, blocked_nouns = BLOCKED_NOUNS, nouns_limit = NOUNS_LIMIT, profiles_table = args.profiles_table,
        trash_words = settings["trash_words"],
        swear_words = settings["swear_words"]
    )

    logging.info("profiles len %s" % len(profiles_dict))
    profiles_dump = {}
    for p in profiles_dict:
        profiles_dump[p] = profiles_dict[p].replys

    json.dump(profiles_dump, open(args.out_file, 'w')) 

if __name__ == '__main__':
    main()


