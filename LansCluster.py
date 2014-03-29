#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys,codecs
import time
import json
import os

import stats

sys.stdout = codecs.getwriter('utf8')(sys.stdout)

db = 'replys_sharper.db'
def main():
    input_file = sys.argv[1]
    clusters_num = int(sys.argv[2])
    output_file = sys.argv[3]

    sim_dict = json.load(open(input_file, "r"))
    for p1 in sim_dict:
        sim_dict[p1][p1] = 0

    cur = stats.get_cursor(db)
    nouns = stats.get_nouns(cur)

if __name__ == '__main__':
    main()
