#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys
import os
import logging, logging.config
import json
from datetime import datetime
import math
import signal

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

def handler(signum, frame):
    logging.error('Signal handler called with signal %s' % signum)
    raise Exception("Timeout")


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

def get_cluster_avg_trend(cluster):
    trend = 0
    for m in cluster["members"]:
        trend += float(m["trend"])

    return trend / len(cluster["members"])

def get_best3_trend(cluster):
    trend = 0
    for m in sorted(cluster["members"], key=lambda x: float(x["trend"]), reverse=True)[:3]:
        trend += float(m["trend"])

    return trend

def get_best_trend(cluster):
    best_trend_cluster = sorted(cluster["members"], key=lambda x: float(x["trend"]))[0]
    return float(best_trend_cluster["trend"])

class TotalFreq:
    def __init__(self, cur, nouns ):
        logging.info("start TotalFreq")
        self.cur = cur
        self.nouns = nouns
        self.init_total_cnt()
        self.init_lemma_freqs()
        logging.info("done TotalFreq")

    def get_nouns_joined(self):
        return ",".join(map(str,self.nouns))

    def init_total_cnt(self):
        self.cur.execute("""
            select sum(cnt), count(*)
            from lwp.lemma_word_pairs l
            where cnt > 1 
            and (noun1_md5 in (%s)
            or noun2_md5 in (%s))
        """ % (self.get_nouns_joined(), self.get_nouns_joined()))

        sum_, cnt = self.cur.fetchone()
        self.total_cnt = sum_

    def init_lemma_freqs(self):
        #logging.info("start")
        self.cur.execute("""
            select noun_md5, source_md5, count(*)
            from tweets_words
            where noun_md5 in (%s)
            group by noun_md5, source_md5
        """ % self.get_nouns_joined())

        lemma_freqs = {}
        for row in self.cur.fetchall():
            n, s, cnt = row
            if n not in lemma_freqs:
                lemma_freqs[n] = {}
            lemma_freqs[n][s] = - math.log(float(cnt) / self.total_cnt, 2)

        self.lemma_freqs = lemma_freqs

    def get_lemmas_for_nouns(self, nouns): 
        lemma_set = []
        for n in nouns:
            lemma_set += self.lemma_freqs[n].keys()

        return set(lemma_set)
                
    
class BagFreq:

    def __init__(self, cur, bag, total_freq):
        logging.info("start BagFreq")
        self.bag = bag
        self.total_freq = total_freq 
        self.cur = cur
        self.word_freqs = {}
        self.pair_freqs = {}
        self.lemma_pair_freqs = {}
        #self.init_total_cnt()
        self.init_word_freqs()
        self.init_pair_freqs()
        #self.init_lemma_freqs()
        self.init_lemma_nexts()
        self.init_lemma_pair_freqs()

        logging.info("done BagFreq")
 
    def get_bag_joined(self):
        s = ",".join(map(str, self.bag)) 
        return s

    def init_word_freqs(self):
        logging.info("start")
        logging.info("Nouns len: %s" % len(self.bag))
        self.cur.execute("""
            select noun_md5, count(*) 
            from tweets_words
            where noun_md5 in (%s)
            group by noun_md5
        """ % (self.get_bag_joined()))

        word_freqs = {}
        while True:
            row = self.cur.fetchone()
            if row is None:
                break

            n, cnt = row
            word_freqs[int(n)] = cnt

        self.word_freqs = word_freqs 
        logging.info("word_freqs len %s" % len(word_freqs.keys()))

        #print self.word_freqs

    def init_pair_freqs(self):
        self.cur.execute("""
            select noun1_md5, noun2_md5, sum(cnt)
            from lwp.lemma_word_pairs
            where noun1_md5 in (%s) 
            and noun2_md5 in (%s)
            and cnt > 1 
            group by noun1_md5, noun2_md5
        """ % (self.get_bag_joined(), self.get_bag_joined()))

        pair_freqs = {}
        while True:
            row = self.cur.fetchone()
            if row is None:
                break

            n1, n2, cnt = row
            if n1 not in pair_freqs:
                pair_freqs[int(n1)] = {}

            pair_freqs[int(n1)][int(n2)] =  - math.log(float(cnt) / self.total_freq.total_cnt, 2)
    
        self.pair_freqs = pair_freqs
            
        #print self.pair_freqs 

        #logging.info("stop")

    def get_lemma_set(self):
        return self.total_freq.get_lemmas_for_nouns(self.bag)       

    def init_lemma_nexts(self):
        #logging.info("start")
        self.cur.execute("""
            select noun1_md5, source2_md5
            from lwp.lemma_word_pairs
            where cnt > 1 
            and noun1_md5 in (%s)
            group by noun1_md5, source2_md5
        """ % (self.get_bag_joined()))
        
        lemma_nexts = {}
        for row in self.cur.fetchall():
            n, l = row
            if n not in lemma_nexts:
                lemma_nexts[n] = []
            lemma_nexts[n].append(l)

        self.lemma_nexts = lemma_nexts

        #logging.info("done nexts")

        self.cur.execute("""
            select noun_md5, source_md5, count(*) as cnt
            from tweets_words
            where noun_md5 in (%s)
            group by noun_md5, source_md5
            order by cnt desc
        """ % (self.get_bag_joined()))

        noun_lemmas = {}
        for row in self.cur.fetchall():
            n, s, cnt = row
            if n not in noun_lemmas:
                noun_lemmas[n] = []
            noun_lemmas[n].append(s)
        self.noun_lemmas = noun_lemmas
        logging.info("noun_lemmas len %s" % len(noun_lemmas.keys()))
        
        #logging.info("stop")

    def init_lemma_pair_freqs(self):
        #logging.info("start")
        self.cur.execute("""
            select noun1_md5, noun2_md5, source1_md5, source2_md5, cnt
            from lwp.lemma_word_pairs l
            where cnt > 1
            and noun1_md5 in (%s)
            and noun2_md5 in (%s)
        """ % (self.get_bag_joined(), self.get_bag_joined()))

        lemma_pair_freqs = {}
        for row in self.cur.fetchall():
            n1, n2, l1, l2, cnt = row
            lemma_freq = - math.log(float(cnt) / self.total_freq.total_cnt, 2)
            
            lemma_pair_freqs[(n1, n2, l1, l2)] = lemma_freq
        
        self.lemma_pair_freqs = lemma_pair_freqs 
        #logging.info("stop")

    def get_chain_lemmas(self, chain):
        chain_lemmas = map(lambda x: None, range(0, len(chain)))
        chain_lemmas[0] = list(set(self.noun_lemmas[chain[0]]) )
        for i in range(0, len(chain)-1):
            suggested = set(self.lemma_nexts[chain[i]])
            available = set(self.noun_lemmas[chain[i+1]])
            chain_lemmas[i + 1] = list(suggested & available)

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

def _get_cur_ind(lens, offset):
    cnt = offset
    ind = []
    for i in range(0, len(lens)):
        ind.append( cnt % lens[i])
        cnt = cnt / lens[i]
    
    assert len(ind) == len(lens)

    return ind

def _get_lemmas_by_ind(ind, chain_lemmas):
    lemmas = []
    for i in range(0, len(ind)):
        lemmas.append(chain_lemmas[i][ind[i]])

    return lemmas

def get_next_tuple(chain_lemmas):
    cnt = 0
    max_offset = 1
    for i in range(0, len(chain_lemmas)):
        max_offset *= len(chain_lemmas[i])
    lens = map(len, chain_lemmas)
    logging.info("get_next_tuple max_offset = %s" % (max_offset))
    if max_offset > 1e5:
        max_offset = 1e5
        logging.error("Too many chain lemmas")
    while cnt < max_offset:
        yield _get_lemmas_by_ind(_get_cur_ind(lens, cnt), chain_lemmas)
        cnt += 1 

def get_lemma_perplexity(chain, lemma, bag):
    perpl = 0
    for i in range(0, len(chain) - 1):
        n1, n2 = chain[i], chain[i+1]
        l1, l2 = lemma[i], lemma[i+1]
        if (n1, n2, l1, l2) in bag.lemma_pair_freqs:        
            perpl += bag.lemma_pair_freqs[(n1,n2,l1,l2)]
        # need a weighted formula
        #elif n1 in bag.total_freq.lemma_freqs and l1 in bag.total_freq.lemma_freqs[n1]:
        #    perpl += bag.total_freq.lemma_freqs[n1][l1]
        else: 
            perpl += MAX_COEF
    return perpl
    
@util.time_logger        
def choose_lemmas(chain, bag):
    chain_lemmas = bag.get_chain_lemmas(chain)
   
    min_perplex = MAX_COEF * len(chain)
    lemmas = []
    for l in get_next_tuple(chain_lemmas):
        perpl = get_lemma_perplexity(chain, l, bag)
        if perpl < min_perplex:
            min_perplex = perpl
            lemmas = l

    return lemmas

def align(bag, nouns):
    logging.info("Input chain: " + u" ".join(map(lambda x: nouns[x], bag.bag)))
    if len(bag.bag) == 1:
        logging.info("One-word chain")
        return [bag.bag]       
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
            if best_pair is not None and best_left_t is not none and best_right_t is not None:
                logging.info("Pairs are not null, perplexities:")
                logging.info("best_f %s, best_l %s, best_r %s" % (best_f, best_l, best_r))
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
        # dunno if this is fail...
        if best_i is not None and best_j is not None:
            chains[best_i] += chains[best_j]
            chains.pop(best_j)
        else:
            logging.warn("Chains glue: there's a tie, forget it")
            break

    logging.debug(chains)

    return chains

def get_aligned_cluster(cur, cur_lemma, cluster, trendy_clusters_limit=20):
    valid_clusters = cluster
    trendy_clusters = sorted(valid_clusters, key=lambda x: get_best3_trend(x), reverse=True)

    cl_nouns = []
    for c in trendy_clusters:
        for m in c["members"]:
            cl_nouns.append(m["id"])
    total_freq = TotalFreq(cur_lemma, cl_nouns)

    new_clusters = []
    empty_chains_count = 0
    for cluster in trendy_clusters[:trendy_clusters_limit]:
        used_nouns = map(lambda x: x["id"], cluster["members"])
        trends = {}
        for m in cluster["members"]:
            trends[m["id"]] = m["trend"]
        nouns = stats.get_nouns(cur, used_nouns)
        
        bag = BagFreq(cur_lemma, used_nouns, total_freq)
    
        chains = []

        chains = align(bag, nouns) 

        sources = stats.get_sources(cur, list(bag.get_lemma_set()))
        logging.info("Got %s chains" % len(chains))
        if len(chains) == 0:
            empty_chains_count += 1
        for c in chains:
            logging.info("Got %s chain elements" % len(c))
            nouns_title = u" ".join(map(lambda x: nouns[x], c))
            lemmas = choose_lemmas(c, bag)
            lemmas_title = u" ".join(map(lambda x: sources[x], lemmas)) 
            members = []
            for i in range(0,len(c)):
                text = sources[lemmas[i]] if len(lemmas) > 0 else nouns[c[i]]
                members.append({"id": c[i], "text": text, "stem_text": nouns[c[i]], "trend": trends[c[i]]})

            member_ids = map(lambda x: x["id"], members) 
            new_clusters.append({
                "members": members, 
                "gen_title": lemmas_title if len(lemmas)>0 else nouns_title,
                "members_len": len(members),
                "members_md5": str(util.digest_large(member_ids)),
                "unaligned": cluster
            })

    sample = sorted(new_clusters, key=lambda x: get_best3_trend(x), reverse=True)

    logging.info("Done; empty_chains_count: %s " % (empty_chains_count))

    return sample

def main():
    parser = util.get_dates_range_parser()
    args = parser.parse_args()

    today = (datetime.utcnow()).strftime("%Y%m%d%H%M%S")
    update_time = (datetime.now()).strftime("%Y-%m-%d %H:%M:%S")

    ind = Indexer(DB_DIR)

    cur = stats.get_main_cursor(DB_DIR)
    cur_display = stats.get_cursor(DB_DIR + "/tweets_display.db")
    cur_date = ind.get_db_for_date(args.start)

    clusters, cluster_date = cur_display.execute("""
        select cluster,cluster_date from clusters order by cluster_date desc limit 1
    """).fetchone()
    clusters = json.loads(clusters)

    sample = get_aligned_cluster(cur, clusters["clusters"]) 

    print json.dumps(sample, indent=4, ensure_ascii=False)

    #cur_display.execute("""
    #    replace into clusters (cluster, cluster_date)
    #    values (?, ?) 
    #""", (json.dumps({"clusters": sample}), cluster_date))
    #print json.dumps(sample, ensure_ascii=False)

    logging.info("Done")

if __name__ == '__main__':
    main()


