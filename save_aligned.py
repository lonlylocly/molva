#!/usr/bin/python
# -*- coding: utf-8 -*-
import logging, logging.config
import json
import codecs
import argparse
from datetime import datetime

import stats
import molva.util as util

logging.config.fileConfig("logging.conf")
logging.getLogger().setLevel(logging.INFO)

settings = {} 
try:
    settings = json.load(open('global-settings.json', 'r'))
except Exception as e:
    logging.warn(e)

DB_DIR = settings["db_dir"] if "db_dir" in settings else os.environ["MOLVA_DIR"]

@util.time_logger
def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--clusters")
    args = parser.parse_args()

    cur_display = stats.get_cursor(DB_DIR + "/tweets_display.db")

    cl = json.load(codecs.open(args.clusters, 'r',encoding="utf8"))
    
    today = (datetime.utcnow()).strftime("%Y%m%d%H%M%S")
    update_time = (datetime.now()).strftime("%Y-%m-%d %H:%M:%S")

    final_cl = {"clusters": cl, "update_time": update_time}
    cl_json = json.dumps(final_cl)
    cur_display.execute("""
        replace into clusters (cluster_date, cluster)
        values (?, ?)
    """, (today, cl_json))


if __name__ == '__main__':
    main()


