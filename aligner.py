#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys
import os
import logging, logging.config
import json
from datetime import datetime
import math

import stats
from Indexer import Indexer
import util

logging.config.fileConfig("logging.conf")
logging.getLogger().setLevel(logging.INFO)

settings = {} 
try:
    settings = json.load(open('global-settings.json', 'r'))
except Exception as e:
    logging.warn(e)

DB_DIR = settings["db_dir"] if "db_dir" in settings else os.environ["MOLVA_DIR"]

MAX_COEF = 1e7

def get_best_trend_cluster(cluster):
    max_trend = 0
    max_i = -1
    for i in range(0, len(cluster["clusters"])):
        c = cluster["clusters"][i]
        if len(c["members"]) == 1:
            continue
        local_trend = 0
        for m in c["members"]:
            if float(m["trend"]) > local_trend:
                local_trend = float(m["trend"])

        if local_trend > max_trend:
            max_trend = local_trend
            max_i = i

    return max_i

class BagFreq:

    def __init__(self, bag, cur):
        self.bag = bag 
        self.cur = cur
        self.word_freqs = {}
        self.pair_freqs = {}
        self.lemma_pair_freqs = {}
        self.init_word_freqs()
        self.init_pair_freqs()
        self.init_lemma_freqs()
        self.init_lemma_nexts()
        #self.init_lemma_pair_freqs()
 
    def get_bag_joined(self):
        return ",".join(map(str, self.bag)) 

    def init_word_freqs(self):
        self.cur.execute("""
            select noun_md5, count(*) 
            from tweets_words
            where noun_md5 in (%s)
            group by noun_md5
        """ % self.get_bag_joined())

        word_freqs = {}
        while True:
            row = self.cur.fetchone()
            if row is None:
                break

            n, cnt = row
            word_freqs[int(n)] = cnt

        self.word_freqs = word_freqs 

        #print self.word_freqs
    
    def init_pair_freqs(self):
        self.cur.execute("""
            select noun1_md5, noun2_md5, cnt
            from word_pairs
            where noun1_md5 in (%s) 
            and noun2_md5 in (%s)
        """ % (self.get_bag_joined(), self.get_bag_joined()))

        pair_freqs = {}
        while True:
            row = self.cur.fetchone()
            if row is None:
                break

            n1, n2, cnt = row
            if n1 not in pair_freqs:
                pair_freqs[int(n1)] = {}

            pair_freqs[int(n1)][int(n2)] =  - math.log(float(cnt) / self.word_freqs[n1], 2)
    
        self.pair_freqs = pair_freqs
            
        #print self.pair_freqs 

    def init_lemma_freqs(self):
        logging.info("start")
        self.cur.execute("""
            select noun_md5, source_md5, count(*)
            from tweets_words
            where noun_md5 in (%s)
            group by noun_md5, source_md5
        """ % self.get_bag_joined())

        lemma_freqs = {}
        for row in self.cur.fetchall():
            n, s, cnt = row
            if n not in lemma_freqs:
                lemma_freqs[n] = {}
            lemma_freqs[n][s] = cnt

        self.lemma_freqs = lemma_freqs
        logging.info("stop")

    def get_best_lemma(self, noun):
        bestie = sorted(self.lemma_freqs[noun].keys(), key=lambda x: self.lemma_freqs[noun][x])[-1]
        return bestie

    def get_bag_lemmas(self):
        lemmas = []
        for n in self.noun_lemmas:
            lemmas += self.noun_lemmas[n]

        return lemmas       

    def init_lemma_nexts(self):
        logging.info("start")
        self.cur.execute("""
            select w.noun1_md5, l.noun_md5
            from word_pairs w
            inner join tweets_words t
            on w.noun2_md5 = t.noun_md5
            inner join sources l
            on t.source_md5 = l.noun_md5
            where w.noun1_md5 in (%s)
            group by w.noun1_md5, l.noun_md5
        """ % (self.get_bag_joined()))
        
        lemma_nexts = {}
        for row in self.cur.fetchall():
            n, l = row
            if n not in lemma_nexts:
                lemma_nexts[n] = []
            lemma_nexts[n].append(l)

        self.lemma_nexts = lemma_nexts

        self.cur.execute("""
            select noun_md5, source_md5
            from tweets_words
            where noun_md5 in (%s)
            group by noun_md5, source_md5
        """ % self.get_bag_joined())

        noun_lemmas = {}
        for row in self.cur.fetchall():
            n, s = row
            if n not in noun_lemmas:
                noun_lemmas[n] = []
            noun_lemmas[n].append(s)
        self.noun_lemmas = noun_lemmas
        
        logging.info("stop")

    def init_lemma_pair_freqs(self):
        logging.info("start")
        self.cur.execute("""
            select t.noun_md5, t2.noun_md5, l.noun1_md5, l.noun2_md5, l.cnt
            from lemma_pairs l
            inner join tweets_words t
            on l.noun1_md5 = t.source_md5
            inner join tweets_words t2
            on l.noun2_md5 = t2.source_md5
            where t.noun_md5 in (%s)
            and t2.noun_md5 in (%s)
        """ % (self.get_bag_joined(), self.get_bag_joined()))

        lemma_pair_freqs = {}
        for row in self.cur.fetchall():
            n1, n2, l1, l2, cnt = row
            lemma_freq = - math.log(float(cnt) / self.word_freqs[n1], 2)
            if (n1, n2) not in lemma_pair_freqs:
                lemma_pair_freqs[(n1, n2)] = []
            
            lemma_pair_freqs[(n1, n2)].append((l1, l2, lemma_freq))
        
        self.lemma_pair_freqs = lemma_pair_freqs 
        logging.info("stop")

    def get_chain_lemmas(self, chain):
        chain_lemmas = map(lambda x: None, range(0, len(chain)))
        chain_lemmas[0] = len(set(self.noun_lemmas[chain[0]]) )
        for i in range(0, len(chain)-1):
            suggested = set(self.lemma_nexts[chain[i]])
            available = set(self.noun_lemmas[chain[i+1]])
            chain_lemmas[i + 1] = len( suggested & available )

        return chain_lemmas

    def getf(self, n1, n2):
        if n1 in self.pair_freqs:
            if n2 in self.pair_freqs[n1]:
                return self.pair_freqs[n1][n2]
        return MAX_COEF

def get_bag_best_pair(neighbours, bag):
    best = MAX_COEF
    best_i = None
    best_j = None
    for i in range(0, len(neighbours)):
        for j in range(i + 1, len(neighbours)):
            f_straight = bag.getf(neighbours[i], neighbours[j])
            if f_straight < best:
                best = f_straight
                best_i = i
                best_j = j
            
            f_reverse = bag.getf(neighbours[j], neighbours[i])
            if f_reverse < best:
                best = f_reverse
                best_i = j
                best_j = i

    if best_i is None or best_j is None:
        return None
           
    return (best, best_i, best_j) 

def get_best_neighbour(neighbours, chains, bag, right=True):
    best = MAX_COEF
    best_n = None
    best_c = None 
    for c in range(0,len(chains)):
        node = chains[c][0] if right else chains[c][-1]
        for n in neighbours:
            f = bag.getf(node, n) if right else bag.getf(n, node)
            if f < best:
                best_n = n
                best_c = c 

    if best_n is None or best_c is None:
        return None    

    return (best, best_n, best_c)

def align(bag, nouns):
    neighbours = map(lambda x: x , bag.bag)
    first_best_pair =  get_bag_best_pair(bag.bag, bag)
    if first_best_pair is None:
        logging.warn("Startup: no good pairs")
        return []

    f, n1, n2 = first_best_pair
    first_best_pair = [neighbours[n1], neighbours[n2]]
    chains = [first_best_pair]
    neighbours.remove(first_best_pair[0])
    neighbours.remove(first_best_pair[1])

    while len(neighbours) > 0:
        best_chain = -1
        best_pair = get_bag_best_pair(neighbours, bag)
        best_left_t = get_best_neighbour(neighbours, chains, bag, right=False) 
        best_right_t = get_best_neighbour(neighbours, chains, bag, right=True) 

        best_f = best_pair[0]    if best_pair is not None else MAX_COEF
        best_l = best_left_t[0]  if best_left_t is not None else MAX_COEF
        best_r = best_right_t[0] if best_right_t is not None else MAX_COEF

        if  best_f < best_l and best_f < best_r:
            new_best_pair = [neighbours[best_pair[1]], neighbours[best_pair[2]]]
            chains.append(new_best_pair)
            neighbours.remove(new_best_pair[0])
            neighbours.remove(new_best_pair[1])
        
        elif best_l < best_f and best_l < best_r:
            best_l, n, c = best_left_t
            chains[c] = [neighbours.pop(n)] + chains[c]
        elif best_r < best_f and best_r < best_r:
            best_l, n, c = best_right_t
            chains[c].append(neighbours.pop(n))
        else:
            logging.warn(u"Не удалось найти пару: " + u" ".join(map(lambda x: nouns[x], neighbours)))
            #logging.warn(chains)
            #logging.warn("Neighbours split: there's a tie, forget it")
            break 

    while len(chains) > 1:
        best_f = MAX_COEF
        best_i = None
        best_j = None
        for i in range(0, len(chains)):
            for j in range(i + 1, len(chains)):
                if bag.getf(chains[i][-1], chains[j][0]) < best_f:
                    best_f = bag.getf(chains[i][-1], chains[j][0])
                    best_i = i
                    best_j = j
                if bag.getf(chains[j][-1], chains[i][0]) < best_f:
                    best_f = bag.getf(chains[j][-1], chains[i][0])
                    best_i = j 
                    best_j = i
        if best_i is not None:
            chains[best_i] += chains[j]
            chains.pop(j)
        else:
            #logging.warn("Chains glue: there's a tie, forget it")
            break

    logging.debug(chains)

    return chains

def choose_lemmas(chain, bag):
    pass

def main():
    parser = util.get_dates_range_parser()
    args = parser.parse_args()

    today = (datetime.utcnow()).strftime("%Y%m%d%H%M%S")
    update_time = (datetime.now()).strftime("%Y-%m-%d %H:%M:%S")

    ind = Indexer(DB_DIR)

    cur = stats.get_cursor(DB_DIR + "/tweets.db")
    cur_display = stats.get_cursor(DB_DIR + "/tweets_display.db")
    cur_date = ind.get_db_for_date(args.start)

    clusters = cur_display.execute("""
        select cluster from clusters order by cluster_date desc limit 1
    """).fetchone()[0]
    clusters = json.loads(clusters)

    for i in range(0,20):
        max_i = get_best_trend_cluster(clusters)
        len(clusters["clusters"])
        cluster = clusters["clusters"].pop(max_i)

        used_nouns = map(lambda x: x["id"], cluster["members"])
        #print json.dumps(cluster, ensure_ascii=False)
        nouns = stats.get_nouns(cur, used_nouns)
        
        bag = BagFreq(used_nouns, cur_date)
    
        logging.info(u"Исходное: " + u" ".join(map(lambda x: nouns[x], bag.bag)))
        chains = align(bag, nouns) 

        for c in chains:
            logging.info(u"Цепочка: " + u" ".join(map(lambda x: nouns[x], c)))
            print bag.get_chain_lemmas(c)


    logging.info("Done")

if __name__ == '__main__':
    main()


