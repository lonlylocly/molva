#!/usr/bin/python
# -*- coding: utf-8 -*-
import math

NOISE_POSITIVE_MATCH_RATE = 0.1 

def compare_floats(f1, f2):
    delta = f1 - f2

    if math.fabs(delta) == 0.0:
        return 0
    else:
        return -1 if delta < 0 else 1


class ProfileCompare:

    def __init__(self, one=None, other=None):
        self.left = one
        self.right = other

        if one is not None and other is not None:
            self.common_tweets = one.common_tweets_with(other) 
        self.dist = None
        self.common_replys = None

        if one is not None and other is not None:
            self.do_compare(one, other)
            self.sim = self.get_sim()

    def do_compare(self, one, other):
        total_uncommon = 0
        tot_com = 0

        #damp1 = one.get_damping_coeff() 
        #damp2 = other.get_damping_coeff() 
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

    def __init__(self, post, reply_min, post_tweet_ids=None):
        self.replys = {}
        self.replys_rel = {}
        self.post = post
        self.total = 0
        self.rel_min = reply_min
        self.post_tweet_ids = post_tweet_ids

    def setup_rel_profile(self):
        self.replys_rel[0] = 0.0
        for reply in self.replys:
            repl_portion = (self.replys[reply] + 0.0)/ self.total
            if repl_portion <= self.rel_min:
                self.replys_rel[0] += repl_portion
            else:
                self.replys_rel[reply] = repl_portion

        if self.post in self.replys_rel:
            del self.replys_rel[self.post]
        damp = self.get_damping_coeff()
        for reply in self.replys_rel:
            if reply == 0:
                continue
            self.replys_rel[reply] = self.replys_rel[reply] * damp
       
        del self.replys_rel[0] 
    
        #self._check_replys_rel()

    def compare_with(self, other):
        return ProfileCompare(self, other) 

    def common_tweets_with(self, other):
        t1 = self.post_tweet_ids
        t2 = other.post_tweet_ids

        common = set(t1) & set(t2)
        total = set(t1) | set(t2)

        return (len(common) + 0.0) / len(total) if len(total) > 0 else 0


    def get_damping_coeff(self):
        coeff = 1
        if 0 in self.replys_rel and self.replys_rel[0] != 1 :
            coeff = 1 / (1 - self.replys_rel[0])

        return coeff       

    def _check_replys_rel(self):       
        ok_cnt = 0
        noise_cnt = 0
        for reply in self.replys_rel:
            if reply == 0:
                noise_cnt += self.replys_rel[reply]
            else:
                ok_cnt += self.replys_rel[reply]

        try: 
            assert (ok_cnt + noise_cnt) <= 1.001 
            assert (ok_cnt + noise_cnt) > 0.999
        except Exception as e:
            print "key: %s; ok_cnt: %s; noise_cnt: %s" % (self.post, ok_cnt, noise_cnt)
            raise e

