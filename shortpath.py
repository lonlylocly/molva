#!/usr/bin/python
# -*- coding: utf-8 -*-

class SmartDict:

    def __init__(self):
        self.d = [] 

    def put(self, p1, p2, s):

        self.d.append((p1, p2, s))

    def get(self, p1, p2):
        for i in self.d:
            if i[0] == p1 and i[1] == p2:
                return s
            if i[1] == p2 and i[0] == p1:
                return s
        
def build_graph(filename):
    f = open(filename, "r")

    t = SmartDict() 

    while True:
        l = f.readline()
        if l is None:
            break

        p1, p2, s = l.split("\n")
        p1 = int(p1)
        p2 = int(p2)
        s = float(s)

        t.put(p1, p2, s)
           
