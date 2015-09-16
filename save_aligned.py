#!/usr/bin/python
# -*- coding: utf-8 -*-
import logging, logging.config
import json
import codecs
import argparse
from datetime import datetime

import molva.stats as stats
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

    final_cl_raw = codecs.open(args.clusters, 'r',encoding="utf8").read()
    final_cl = json.loads(final_cl_raw)

    cur_display.execute("""
        replace into clusters (cluster_date, cluster)
        values (?, ?)
    """, (final_cl["cluster_id"], final_cl_raw))


if __name__ == '__main__':
    main()


