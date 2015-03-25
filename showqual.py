#!/usr/bin/python
# -*- coding: utf-8 -*-
import json
from datetime import datetime as d

import stats

settings = {} 
try:
    settings = json.load(open('global-settings.json', 'r'))
except Exception as e:
    logging.warn(e)

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

    def put_mark(self, mark):
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

    def __str__(self):
        names = self.get_metric_names()
        s = "%20s %30s " % ("update_time", "username")
        for n in names:
            s += "%20s " % n
        s += "\n"
        for k in sorted(self.marks):
            s += "%20s %30s " % k
            m = self.marks[k]
            av = m.get_metric_average()
            for n in names:
                val = ""
                if n in av:
                    val = av[n]
                s += "%20s" % val
            s += "\n"

        return s

        

def get_marks(cur):
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
            markset.put_mark(mark)

        marks_all.put_markset(markset)

    return marks_all 
        

def main():
    cur = stats.get_cursor(settings["db_dir"] + "/quality_marks.db")
    
    m = get_marks(cur)
    print m

    return
       

if __name__ == '__main__':
    main()
