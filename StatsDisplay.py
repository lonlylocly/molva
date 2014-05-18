#!/usr/bin/python
# -*- coding: utf-8 -*-
import logging

import stats


class StatsDisplay:

    def __init__(self, db_dir):
        self.db_dir = db_dir
        self.main_db = stats.get_cursor(db_dir + "/tweets.db")

    def get_table_stats(self):
        res = self.main_db.execute("select table_date, table_name, row_count from table_stats").fetchall()

        table_stats = {}
        for r in res:
            table, date, count = r
            if date not in table_stats:
                table_stats[date] = {}
            table_stats[date][table] = count

        return table_stats 
    
