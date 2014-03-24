#!/usr/bin/python
# -*- coding: utf-8 -*-

import time
import stats
import codecs

class GraphEdge:

    def __init__(self, p1, p2, s):
        self.p1 = p1
        self.p2 = p2
        self.s = s

    def __str__(self):
        return '"%d" -> "%d" [label="%f"];' % (self.p1, self.p2, self.s)

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
        min_s = 1000
        min_p2 = None
        for p2 in set(self.d[p].keys()) - set(ps_to_exclude):
            if self.d[p][p2] < min_s:
                min_s = self.d[p][p2]
                min_p2 = p2

        return GraphEdge(p, min_p2, min_s)
   
    def get_closest_to(self, graph):
        l = []
        ps = map(lambda x: x.p1, graph)
        ps += map(lambda x: x.p2, graph)
        for p in set(ps):
            edge = self._get_closest_to(p, ps)
            if edge.p2 is not None:
                l.append(edge)

        l = sorted(l, key=lambda x: x.s)

        if len(l) == 0:
            return None
        else:
            return l[0]
         
        
    def _init_dict(self, filename):
        print "[%s] _init_dict " % (time.ctime())
        f = open(filename, "r")
        
        self.min_edge = GraphEdge(None, None, None)
        while True:
            l = f.readline()
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

                if p1 not in self.d:
                    self.d[p1] = {}
                if p2 not in self.d:
                    self.d[p2] = {}

                self.d[p1][p2] = s
                self.d[p2][p1] = s 
            except Exception as e:
                print l
                raise e

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
    nouns = stats.get_nouns(cur)
 
    d = SmartDict("sim.txt")
    f = codecs.open("graph.dot", "w",encoding='utf8')
    g = d.build_graph() 

    write_dot(g, nouns, f)

    f.close()
    print "[%s] Done " % (time.ctime())

if __name__ == '__main__':
    main()
