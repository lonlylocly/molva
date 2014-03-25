#!/usr/bin/python
# -*- coding: utf-8 -*-

import time
import stats
import codecs
import json

class GraphEdge:

    def __init__(self, p1=None, p2=None, s=None):
        self.p1 = p1
        self.p2 = p2
        self.s = s

    def __str__(self):
        return json.dumps({"p1": self.p1, "p2": self.p2, "s": self.s}) 

    def from_json(self, s):
        j = json.loads(s)
        self.p1 = j["p1"]
        self.p2 = j["p2"]
        self.s = j["s"]
        
        return self

class SmartDict:

    def __init__(self, filename):
        self.d = {} 
        self._init_dict(filename)

    def put(self, p1, p2, s):

        self.d.append((p1, p2, s))

    def get(self, p1, p2):
        for i in self.d:
            if i[0] == p1 and i[1] == p2:
                return s
            if i[1] == p2 and i[0] == p1:
                return s
    
    def _get_closest_to(self, p, ps_to_exclude):
        
        res_edge = None
        while True:
            if len(self.d[p]) == 0:
                break
            edge = self.d[p][0]
            if edge.p2 not in ps_to_exclude:
                res_edge = edge
                break
                
            self.d[p].pop(0)
                
        return res_edge
   
    def get_closest_to(self, graph):
        l = []
        ps = map(lambda x: x.p1, graph)
        ps += map(lambda x: x.p2, graph)

        min_edge = None 
        ps = set(ps)
        for p in ps :
            edge = self._get_closest_to(p, ps) 
            if edge is None:
                continue
            if min_edge is None:
                min_edge = edge
            if edge.s < min_edge.s:
                min_edge = edge

        if min_edge is not None:
            self.d[min_edge.p1].pop(0)

        return min_edge
        
    def _init_dict(self, filename):
        print "[%s] _init_dict " % (time.ctime())
        f = open(filename, "r")
        
        d = {}
        self.min_edge = GraphEdge(None, None, None)
        long_cnt = 0
        cnt = 0

        ps = set()
 
        while True:
            l = f.readline()
            cnt += 1
            if cnt > long_cnt * 1e6:
                print "[%s] read %d lines" % (time.ctime(), cnt)
                long_cnt += 1

            if l is None or l == "":
                break

            try:
                p1, p2, s = l.split("\t")
                p1 = int(p1)
                p2 = int(p2)
                s = float(s)
                if self.min_edge.s is None or s < self.min_edge.s:
                    self.min_edge.s = s
                    self.min_edge.p1 = p1
                    self.min_edge.p2 = p2

                ps.add(p1) 
                ps.add(p2)
                if p1 > p2:
                    if p2 not in d:
                        d[p2] = {}

                    d[p2][p1] = s
                else:
                    if p1 not in d:
                        d[p1] = {}

                    d[p1][p2] = s                   

            except Exception as e:
                print l
                raise e
        ps_list = list(ps) 
        for i in range(0, len(ps_list)):
            p = ps_list[i]
            l = []

            for j in range(i + 1, len(ps_list)):
                p2 = ps_list[j]
                if p > p2:
                    l.append(GraphEdge(p, p2, d[p2][p]))
                else: 
                    l.append(GraphEdge(p, p2, d[p][p2]))
            self.d[p] = sorted(l, key=lambda x: x.s)

        print "[%s] _init_dict (done)" % (time.ctime())

    def build_graph(self):
        print "[%s] build_graph " % (time.ctime())

        g = [self.min_edge]   
            
        while True:
            print "[%s] iteration #%d " % (time.ctime(), len(g))
            new_edge = self.get_closest_to(g)
            if new_edge is None:
                break

            g.append(new_edge)

    
        print "[%s] build_graph done " % (time.ctime())

        return g

def write_json(g, nouns, f):
    for edge in g:
        f.write(edge.__str__())
        f.write("\n")

def read_json( f):
    g = []
    while True:
        l = f.readline()
        if l is None or l == "":
            break
        g.append(GraphEdge().from_json(l))

    return g


def write_stats(g, nouns, f):
    ll = [] 
    for noun in nouns.keys():
        l = filter(lambda e: e.p1 == noun or e.p2 == noun, g)
        ll.append((noun, len(l)))

    ll = sorted(ll, key=lambda x: x[1])
    for l in ll:
        if l[1] == 0:
            continue
        f.write("%s - %d\n" % (nouns[l[0]], l[1]))

def write_dot(g, nouns, f):
    f.write("digraph G _{\n")

    ps = map(lambda x: x.p1, g)
    ps += map(lambda x: x.p2, g)

    for p in set(ps):
        f.write('"%d" [label="%s"];\n' % (p, nouns[p]))

    for edge in sorted(g, key=lambda x: x.s):
        f.write(str(edge))
        f.write("\n")

    f.write("}\n")
   

def main():
    print "[%s] Startup " % (time.ctime())

    cur = stats.get_cursor("replys_sharper.db")   
    nouns = {} # stats.get_nouns(cur)
 
    d = SmartDict("sim.txt")
    f = codecs.open("graph.txt", "w",encoding='utf8')
    g = d.build_graph() 

    #write_dot(g, nouns, f)
    write_json(g, nouns, f)

    f.close()
    print "[%s] Done " % (time.ctime())

if __name__ == '__main__':
    main()
