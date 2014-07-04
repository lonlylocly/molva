#!/usr/bin/python
# -*- coding: utf-8 -*-
import math
import numpy


class ProfileCompare:

    def __init__(self, one=None, other=None, check_common=False):
        self.left = one
        self.right = other

        if check_common and one is not None and other is not None:
            self.common_tweets = one.common_tweets_with(other) 
        else:
            self.common_tweets = None
 
        self.dist = None
        self.common_replys = None

        if one is not None and other is not None:
            #self.do_compare(one, other)
            #self.get_spearman(one, other)
            self.dist = self.cosine(one, other)
            self.sim = self.dist

    def do_compare(self, one, other):
        total_uncommon = 0
        tot_com = 0

        com_set = (set(one.replys_rel.keys()) | set(other.replys_rel.keys()))
        for reply in com_set:
            if reply == 0:
                continue
            #x1 = damp1 * one.replys_rel[reply] if reply in one.replys_rel else 0 
            #x2 = damp2 * other.replys_rel[reply] if reply in other.replys_rel else 0 
            x1 = one.replys_rel[reply] if reply in one.replys_rel else 0 
            x2 = other.replys_rel[reply] if reply in other.replys_rel else 0
            #tot_com += min(x1, x2)

            total_uncommon += math.fabs(x1 - x2)
            #total_uncommon += math.fabs(1/x1 - 1/x2)
        
        total_uncommon = total_uncommon /2 # do i need this ?
        self.dist = total_uncommon # / len(com_set)  

    def vect_norm(self, vect):
        norm = 0
        for i in vect:
            norm += math.sqrt(i ** 2)

        return norm

    def cosine(self, one, other):
            
        com_set = (set(one.replys.keys()) | set(other.replys.keys()))
        x1 = []
        x2 = []
       
        for reply in com_set:
            x1.append(one.replys[reply] if reply in one.replys else 0) 
            x2.append(other.replys[reply] if reply in other.replys else 0)
        
        cos = numpy.dot(x1, x2) / (numpy.linalg.norm(x1) * numpy.linalg.norm(x2))
        #print x1
        #print x2
        #print cos

        #raise Exception("stop")

        return 1 - cos

    def get_spearman(self, one, other):
        sum_spearman = 0
        
        com_set = (set(one.replys_rel.keys()) | set(other.replys_rel.keys()))
        k = 6.0 / (len(com_set) * (len(com_set)**2  -1 ))
        for reply in com_set:
            if reply == 0:
                continue
            x1 = one.replys_rel[reply] if reply in one.replys_rel else 0 
            x2 = other.replys_rel[reply] if reply in other.replys_rel else 0
            
            sum_spearman += (x1 - x2 ) ** 2

        rs = 1 - k * sum_spearman 
        
        self.dist = rs
        return  rs


    def get_sim(self):
        return self.dist # + self.common_tweets
    
    def __str__(self):
        return u"Похожесть: %f; Общих твитов: %f" % (self.dist, self.common_tweets)         

class NounProfile:

    #@staticmethod
    #def from_json():
    #    d = json.loads(s)
    #    p = NounProfile(0, 0)
    #    p.replys = d["replys"]

    def __init__(self, post, reply_min=None, post_tweet_ids=None, post_cnt=None):
        self.replys = {}
        self.replys_rel = {}
        self.post = post
        self.total = 0
        self.rel_min = reply_min
        self.post_tweet_ids = post_tweet_ids
        self.post_cnt = post_cnt

    def compare_with(self, other):
        return ProfileCompare(self, other) 

    def common_tweets_with(self, other):
        t1 = self.post_tweet_ids
        t2 = other.post_tweet_ids

        common = set(t1) & set(t2)
        total = set(t1) | set(t2)

        return (len(common) + 0.0) / len(total) if len(total) > 0 else 0

    def apply_log(self):
        for r in self.replys:
            self.replys[r] = math.log(self.replys[r] + 1, 2) 

    def divide_post_cnt(self, post_cnt):
        for r in self.replys:
            self.replys[r] = self.replys[r] / math.log(post_cnt, 2) 

    def print_replys(self, nouns):
        for r in self.replys:
            print "%s: %s" % (nouns[r], self.replys[r])

    def add_profile(self, other, w=1):
        for r in other.replys:
            if r not in self.replys:
                self.replys[r] = 0
            self.replys[r] += w * other.replys[r]

