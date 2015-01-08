#!/usr/bin/python
# -*- coding: utf-8 -*-
import argparse
import codecs
import re
import sys
import logging

import util
import intercorr
import stats


sys.stdout = codecs.getwriter('utf8')(sys.stdout)

class SimMap:

    def __init__(self):
        self.sims = {}
        self.min_sim = 100
        self.max_sim = 0

    def put(self, n1, n2, sim):
        n1 = int(n1)
        n2 = int(n2)
        if n1 not in self.sims:
            self.sims[n1] = {}
        if n2 not in self.sims:
            self.sims[n2] = {}   
        sim = float(sim.replace(',','.'))
        sim = 10 * (1 - sim)
        
        if sim > self.max_sim:
            self.max_sim = sim
        if sim < self.min_sim:
            self.min_sim = sim

        self.sims[n1][n2] = sim 
        self.sims[n2][n1] = sim

    def norm(self):
        
        #for n1 in self.sims:
        #    for n2 in self.sims[n1]:        
        #        self.sims[n1][n2] = (self.sims[n1][n2] - self.min_sim) / (self.max_sim - self.min_sim)
        pass


def get_translations(translate):
    tr = open(translate, 'r').read()
    tr_words = []
    tr_total = []
    tr_map = {}
    for l in tr.split("\n"):
        if l == "":
            break
        tr_total.append(l)
        word, translate = l.split('\t') 
        if not translate.startswith('-'):
            translate = re.sub(',.*', '', translate)
            translate = re.sub('\s.*', '', translate)
            tr_words.append(word)
            tr_map[word] = translate

    print "Total rows: %s; translate available: %s" % (len(tr_total), len(tr_words))

    return tr_map

def get_good_etalons(weights, tr_map, sim_map):
    wg = open(weights,'r').read()
    good_wgs = []
    for l in wg.split("\n"):
        if l == "":
            break
        n1, n2, w = l.split("\t")
        w = re.sub('\s.*', '', w)
        if n1 in tr_map.keys() and n2 in tr_map.keys():
            tr1 = tr_map[n1].decode('utf8')
            tr2 = tr_map[n2].decode('utf8')
            n1_d = util.digest(tr1)
            n2_d = util.digest(tr2)
            if n1_d == n2_d:
                continue
            if n1_d not in sim_map:
                print u"not found '%s' at sim_map" % tr1
                continue
            if n2_d not in sim_map[n1_d]:
                print u"not found '%s' at sim_map" % tr2
                continue
            dev=str(float(w) - float(sim_map[n1_d][n2_d])) 
            approx = str(sim_map[n1_d][n2_d])
            good_wgs.append((n1, tr1, n2, tr2, w, approx, dev.replace('.',',')))

    print "Etalon pairs availale: %s" % len(good_wgs)

    return good_wgs

def get_measured(measured):
    sims = open(measured,'r')

    sim_map = SimMap()
    while True:
        l = sims.readline()
        if l is None or l == '':
            break
        n1, n2, sim = l.split(';')
        sim_map.put(n1, n2, sim)

    sim_map.norm()

    return sim_map

def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("-w", "--weights", default="wordsim353/combined.tab")
    parser.add_argument("-t", "--translate", default="wordsim353/translate.txt")
    parser.add_argument("-m", "--measured")
    parser.add_argument("--store")
    parser.add_argument("--alias")
    args = parser.parse_args()

    print args

    weights = args.weights 
    translate = args.translate
    measured = args.measured

    sim_map = get_measured(measured)
    tr_map = get_translations(translate)

    print "min sim: %s; max sim: %s " % (sim_map.min_sim, sim_map.max_sim)

    good_weights = get_good_etalons(weights, tr_map, sim_map.sims)

    m1 = []
    m2 = []
    for g in good_weights:
        g = list(g)
        m1.append(float(g[4]))
        m2.append(float(g[5]))
        g[4] = g[4].replace('.',',')
        g[5] = g[5].replace('.',',')
        print "\t".join(g)

    #print m1
    #print m2

    r_s = intercorr.get_spearman(m1, m2)

    print "Spearman: %s" % r_s    

    r_p = intercorr.get_pearson(m1, m2)

    print "Pearson: %s" % r_p 

    if args.store:
        cur = stats.get_cursor(args.store)
        #cur.execute("create table if not exists torus ( date_id text, pearson float, spearman float)")
        #cmd = "insert into torus values ('%s', %s, %s)" % (args.alias, r_p, r_s)
        #logging.info(cmd)
        #cur.execute(cmd)

        cur.execute("create table if not exists date_wordsim (date_id text, pair text, sim float, primary key(date_id, pair))")
        cur.execute("delete from date_wordsim where date_id = '%s'" % (args.alias))
        for g in good_weights:
            g = list(g)
            #g[4] = g[4].replace('.',',')
            #g[5] = g[5].replace('.',',')
            #print "\t".join(g)
            cur.execute("insert into date_wordsim values ('%s', '%s', %s)" % (args.alias, " ".join(g[:4]), float(g[5])))
            cur.execute("insert or ignore into date_wordsim values ('%s', '%s', %s)" % ("wordsim", " ".join(g[:4]), float(g[4])))



if __name__ == '__main__':
    main()
