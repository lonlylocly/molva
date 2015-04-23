#!/usr/bin/python
# -*- coding: utf-8 -*-
import json
from datetime import datetime as d
import logging
import argparse

import molva.stats as stats

logging.config.fileConfig("logging.conf")

settings = {} 
try:
    settings = json.load(open('global-settings.json', 'r'))
except Exception as e:
    logging.warn(e)

minmax = {
    'title_correctness': {'min': 0, 'max': 3},
    'topic_relevance': {'min': 0, 'max': 3},
    'tweets_relatedness': {'min': 0, 'max': 3}
}


class Mark:
    def __init__(self, topic=None, metrics=None):
        self.topic = topic
        self.metrics = metrics if metrics is not None else {}

    def put_metric(self, name, val):
        self.metrics[name] = val

    def to_json(self):
        return {
            "topic":   self.topic,
            "metrics": self.metrics
        }

    def __str__(self):
        return json.dumps(self.to_json())

class MarkSet:
    def __init__(self, upd=None, username=None, marks=None):
        self.update_time = upd
        self.username = username
        self.marks = marks if marks is not None else []

        self.metric_sum = {}

    def put_mark(self, mark, filter_spam=False):
        if filter_spam and "spam" in mark.metrics and mark.metrics["spam"] == "1":
            logging.info("Skip mark - it is spam")
            return
        self.marks.append(mark)

        for name in mark.metrics:
            if name not in self.metric_sum:
                self.metric_sum[name] = 0 
            self.metric_sum[name] += int(mark.metrics[name])

    def get_key(self):
        return (self.update_time, self.username)

    def to_json(self):
        return {
            "update_time": self.update_time,
            "username":    self.username,
            #"marks": map(lambda x: x.to_json(), self.marks),
            "av": self.get_metric_average()
        }

    def __str__(self):
        return json.dumps(self.to_json())

    def get_metric_average(self):
        av = {}
        for n in self.metric_sum:
            av[n] = float(self.metric_sum[n]) / len(self.marks)
    
        return av

    def get_metric_names(self):
        return self.metric_sum.keys()

class Marks:
    def __init__(self):
        self.marks = {}

    def put_markset(self, markset):
        if markset.get_key() not in self.marks:
            self.marks[markset.get_key()] = markset
        else:
            self.marks[markset.get_key()] = markset
            print "key %s already in Marks" % str(markset.get_key())

    def get_metric_names(self):
        names = []
        for m in self.marks:
            names += self.marks[m].get_metric_names()

        names = list(set(names))

        return names

    def get_stat_matrix(self):
        names = self.get_metric_names()
        stat = [["update_time", "username"] + names + ['total']]
        for k in sorted(self.marks):
            m = self.marks[k]
            av = m.get_metric_average()
            total_mark = 0
            metric_cnt = 0
            stat_row = list(k)
            for n in names:
                val = ""
                if n in av:
                    val = "%.2f" % float(av[n])
                    if n in minmax:
                        total_mark += float(val) / minmax[n]["max"]
                        metric_cnt += 1
                stat_row.append(val)
            stat_row.append("%.2f" % (total_mark / metric_cnt * 100))
            stat.append(stat_row)
        return stat

    def get_col_sizes(self, stat):
        sizes = map(lambda x: 0, range(0,len(stat[0])))
        for i in range(0, len(stat)):
            for j in range(0, len(stat[i])):
                if len(stat[i][j]) > sizes[j]:
                    sizes[j] = len(stat[i][j])
        return sizes

    def __str__(self):
        stat = self.get_stat_matrix()
        sizes = self.get_col_sizes(stat)
        sizes_patt = map(lambda x: "%%%ds" % (x+2), sizes)
        patt = " ".join(sizes_patt)
        return "\n".join(map(lambda x: patt % tuple(x),stat))
                

def get_marks(cur, filter_spam):
    cur.execute("""
        select  update_time, username, marks
        from quality_marks
    """)

    marks_all = Marks()
    while True:
        r= cur.fetchone()
        if r is None:
            break
        upd, user, marks = r
        upd = d.strftime(d.strptime(str(upd), "%Y%m%d%H%M%S"), "%Y-%m-%d %H:%M:%S")
        markset = MarkSet(upd, user)
        marks = json.loads(marks)

        for m in marks:
            mark = Mark(m["topic_md5"])
            for k in m:
                if k == "topic_md5":
                    continue
                mark.put_metric(k, m[k]) 
            markset.put_mark(mark, filter_spam)

        marks_all.put_markset(markset)

    return marks_all 
        

def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--filter-spam", action="store_true")

    args = parser.parse_args()

    cur = stats.get_cursor(settings["db_dir"] + "/quality_marks.db")
    
    m = get_marks(cur, args.filter_spam)
    print m

    return
       

if __name__ == '__main__':
    main()
