#!/usr/bin/python
# -*- coding: utf-8 -*-
import sqlite3
import sys,codecs
import time
import math
import json
import re
import os

from stats import get_tweets_nouns, get_post_replys_tweets, get_nouns, get_post_tweets_cnt 
from util import digest

from profile import NounProfile, ProfileCompare

sys.stdout = codecs.getwriter('utf8')(sys.stdout)

db = 'replys_sharper.db'

TIMESTAMP = time.strftime("%m_%d_%H_%M_%S", time.gmtime())

BASE_DIR = "./sim-net/cosine/"


REPLY_REL_MIN = 0.005
POST_MIN_FREQ = 100
REPLY_MIN_FREQ = 1 

BLOCKED_NOUNS_LIST = u"""
"""
BLOCKED_NOUNS_LIST2 = u"""
до
ли

уж
че
ага
нет
чё
эта
чё-то
оха
до
"""
BLOCKED_NOUNS_LIST += u"\n".join(list(u"абвгдеёжзиклмнопрстуфхцчшщыьъэюя"))

BLOCKED_NOUNS = ",".join(map( lambda x: str(digest(x)), BLOCKED_NOUNS_LIST.split("\n")))

def get_noun_profiles(cur, post_min_freq = POST_MIN_FREQ):
    stats = cur.execute("""
        select p.post_md5, p.reply_md5, p.reply_cnt
        from post_reply_cnt p
        inner join post_cnt p2
        on p.post_md5 = p2.post_md5
        where p2.post_cnt > %d
        and p.post_md5 not in (%s) 
        and p.reply_md5 not in (%s)
    """ % (post_min_freq, BLOCKED_NOUNS, BLOCKED_NOUNS)).fetchall()

    profiles_dict = {}

    for s in stats:
        post = int(s[0])
        reply = int(s[1])
        cnt = int(s[2])
        if post not in profiles_dict:
            profiles_dict[post] = NounProfile(post, REPLY_REL_MIN) 
        profiles_dict[post].replys[reply] = cnt

    return profiles_dict

def set_noun_profiles_tweet_ids(profiles_dict, tweet_nouns):
    for x in profiles_dict.keys():
        profiles_dict[x].post_tweet_ids = tweet_nouns[x] 

def set_noun_profiles_total(cur, profiles_dict, post_min_freq = POST_MIN_FREQ):
    total_stats = cur.execute("""
        select p.post_md5, sum(p.reply_cnt)
        from post_reply_cnt p
        inner join post_cnt p2
        on p.post_md5 = p2.post_md5
        where p2.post_cnt > %d
        and p.post_md5 not in (%s)
        and reply_md5 not in (%s)
        group by p.post_md5
    """ % (post_min_freq, BLOCKED_NOUNS, BLOCKED_NOUNS)).fetchall()

    for s in total_stats:
        post = int(s[0])
        reply_cnt = int(s[1])

        #assert post in profiles_dict
        if post not in profiles_dict:
            continue

        profiles_dict[post].total = reply_cnt 

def set_noun_profiles_total_(cur, profiles_dict):
    total_stats = cur.execute("""
        select p_md5, count(*) from (
            select p.p_md5, p.r_id 
            from chains_nouns p   
            inner join post_cnt p2
            on p.p_md5 = p2.post_md5
            where p2.post_cnt > %d
            and p.p_md5 not in (%s)
            group by p.p_md5, p.r_id
        )
        group by p_md5 
    """ % (POST_MIN_FREQ, BLOCKED_NOUNS)).fetchall()

    for s in total_stats:
        post = int(s[0])
        reply_cnt = int(s[1])

        #assert post in profiles_dict
        if post not in profiles_dict:
            continue

        profiles_dict[post].total = reply_cnt 

def get_synt_common_profile(profiles_dict):
    synt = {}
    for p in profiles_dict.values():
        for r in p.replys_rel.keys():
            if r not in synt:
                synt[r] = 0
            synt[r] += p.replys_rel[r]

    plen = len(profiles_dict.keys())
    for r in synt.keys():
        synt[r] = synt[r] / plen

    return synt


def set_rel_stats(profiles_dict):
    for post in profiles_dict:
        profiles_dict[post].setup_rel_profile() 
         
def debug_sim_net(profiles_dict, nouns, tweets_nouns):
    f = open("sim.txt", "r")

    posts = profiles_dict.keys()

    cnt = 0
    long_cnt = 0

    top_sims = {}
    total_sims = {} 
    sim_dict = {}
    for i in range(0, len(posts)):
        post1 = posts[i]
        cmps = []
        sim_dict[post1] = {} 
        for j in range(0, len(posts)):
            if i == j:
                continue
            post2 = posts[j]
            p_compare = profiles_dict[post1].compare_with(profiles_dict[post2])
            #write_compare_info(post1, post2, profiles_dict, nouns)
            cnt += 1
            cmps.append(p_compare)
            sim_dict[post1][post2] = p_compare.sim
        cmps = sorted(cmps, key=lambda x: x.sim)

        total_sim = write_noun_sim_info( profiles_dict[post1], cmps, nouns, tweets_nouns)
        total_sims[post1] = total_sim

        top_sims[post1] = cmps[0] 
   
    f = open(BASE_DIR + "/sims.json", "w")
    f.write(json.dumps(sim_dict, indent=4))
    f.close()
    #f.close()

    return top_sims 

def get_common_tweet_ratio(post1, post2, tweet_nouns):
    t1 = tweet_nouns[post1]
    t2 = tweet_nouns[post2]

    common = set(t1) & set(t2)
    total = set(t1) | set(t2)

    return (len(common) + 0.0) / len(total)

def get_compare_info_filename(post1, post2, base_dir=BASE_DIR):
    return base_dir + str(post1) + "-" + str(post2) + ".html"


def write_compare_info(post1, post2, profiles_dict, nouns):
    filename = get_compare_info_filename(post1, post2) 
    print "[%s] write file %s " % (time.ctime(), filename)
    fout = codecs.open(filename,'w',encoding='utf8')
    fout.write("<html><head><meta charset=\"UTF-8\"><title>" + nouns[post1] + "-" + nouns[post2] + "</title></head><body>")

    fout.write(u"<table border=1><tr><th>Reply</th><th>%s</th><th>%s</th><th>Общее</th><th>Различное</th><th>new_f</th></tr>" % (nouns[post1], nouns[post2]))
 
    r1 = profiles_dict[post1].replys_rel
    r2 = profiles_dict[post2].replys_rel
    common_replys = set(r1.keys()) & set(r2.keys())   
    all_replys = (set(r1.keys()) | set(r2.keys())   ) - common_replys
    total_min = 0
    total_diff = 0
    for r in list(common_replys) + list(all_replys):
        pr1 = r1[r] if r in r1 else 0
        pr2 = r2[r] if r in r2 else 0
        noun_text = nouns[r] if r != 0 else u"шум"
        pr_min = min(pr1, pr2)
        pr_diff = math.fabs(pr1 - pr2)
        new_f = math.fabs( (1/ (pr1 if pr1 != 0 else REPLY_REL_MIN)) - (1/(pr2 if pr2 != 0 else REPLY_REL_MIN)))
        fout.write(u'<tr>')
        fout.write(u'<td>%s</td>' %( noun_text))
        for i in [(pr1, "red"), (pr2, "blue"), (pr_min, "green"), (pr_diff, "black"), (new_f, "violet")]:
            fout.write(u'<td style="width: 200px"> <div style="width: %d%%; background-color: %s;">&nbsp</div>%f</td>'   % (int(100 * i[0]), i[1], i[0]))
        fout.write(u'</tr>')
        total_min += pr_min
        total_diff += pr_diff
              
    fout.write(u"<tr><td><b>Summary</b></td><td></td><td></td><td>%f</td><td>%f</td></tr>" % (total_min, total_diff))
    fout.write("</table>")

    footer = """
</body></html>
"""
    fout.write(footer)    
    fout.close() 

def write_target_noun_info(post_profile, nouns, tweets_nouns):

    post_tweet_cnt = len(set(tweets_nouns[post]))
    filename = BASE_DIR + str(post) + ".html"
    print "[%s] write file %s " % (time.ctime(), filename)
    fout = codecs.open(filename,'w',encoding='utf8')
    fout.write("<html><head><meta charset=\"UTF-8\"><title>" + nouns[post] + "</title></head><body>")

    fout.write("<h2>" + nouns[post] + "<h2>\n")
    fout.write(u"<p>Всего постов с этим словом: %d</p>" % post_tweet_cnt )
    fout.write(u"<h3>Топ-10 похожих постов</h3>")
    for i in cmps:
        post2 = i.right.post
        cmp_filename = get_compare_info_filename(post, post2, base_dir="./")
        fout.write(u'<a href="./%d.html">%s</a> - %s <a href="%s">%s</a><br/>\n' % (post2, nouns[post2], i.sim, cmp_filename, cmp_filename ))
        write_compare_info(post, post2, profiles_dict, nouns)
    
    #fout.write(u"<h3>Профиль ответов на пост</h3>")
        footer = """
</body></html>
"""
    fout.write(footer)    
    fout.close() 


def write_noun_sim_info(post_profile, cmps, nouns, tweets_nouns):
    post = post_profile.post
    profile = post_profile.replys_rel

    post_tweet_cnt = len(set(tweets_nouns[post]))
    filename = BASE_DIR + str(post) + ".html"
    print "[%s] write file %s " % (time.ctime(), filename)
    fout = codecs.open(filename,'w',encoding='utf8')
    fout.write("<html><head><meta charset=\"UTF-8\"><title>" + nouns[post] + "</title></head><body>")

    fout.write("<a href='./index.html'>./index.html</a>\n")
    fout.write("<h2>" + nouns[post] + "<h2>\n")
    fout.write(u"<p>Всего постов с этим словом: %d</p>" % post_tweet_cnt )
    fout.write(u"<h3>Похожесть постов</h3>")
    fout.write(u"<table><tr><th>Пост</th><th>Похожесть</th><th>Детали</th></tr>")
    total_sim = 0.0
    for i in cmps:
        post2 = i.right.post
        total_sim += i.sim
        fout.write(u'<tr><td><a href="./%d.html">%s</a></td><td>%s</td><td>(<a href="./%d-%d.html">details</a>)</td></tr>\n' % (post2, nouns[post2], i, post, post2))
    
    fout.write(u"</table>")
    fout.write(u"<p>Сумма всех похожестей: %f</p>" % total_sim)

    fout.write(u"<h3>Профиль ответов на пост</h3>")
    profile_parts = reversed(sorted(profile.keys(), key=lambda x: float(profile[x])))
    
    for reply in profile_parts:
        p = profile[reply]    
        reply_text = nouns[reply] if reply != 0 else u"<i>шум</i>"
        fout.write('<a href="./%d.html">%s</a> - %.6f (%d)<br/>\n' % (reply, reply_text, p, post_profile.replys[reply]))

    footer = """
</body></html>
"""
    fout.write(footer)    
    fout.close() 
    
    return total_sim

def write_noun_sim_index(posts, nouns, top_sims):
    filename = BASE_DIR + "index.html"
    print "[%s] write file %s " % (time.ctime(), filename)
    fout = codecs.open(filename,'w',encoding='utf8')
    fout.write(u"<html><head><meta charset=\"UTF-8\"><title>Индекс словопостов</title></head><body>")

    fout.write(u"<h3>Индекс словопостов<h3>\n")

    for t in sorted(posts, key=lambda x: top_sims[x]):
        fout.write('<a href="./%d.html">%s</a> %f<br/>\n' % (t, nouns[t], top_sims[t].sim))

    footer = """
</body></html>
"""
    fout.write(footer)    
    fout.close() 

def write_noun_sim_index2(sample_profiles, target_profiles, nouns):
    filename = BASE_DIR + "classify.html"
    print "[%s] write file %s " % (time.ctime(), filename)
    fout = codecs.open(filename,'w',encoding='utf8')
    fout.write(u"<html><head><meta charset=\"UTF-8\"><title>Индекс словопостов</title></head><body>")

    fout.write(u"<h3>Индекс словопостов<h3>\n")

    for post1 in target_profiles.keys():
        p1p = target_profiles[post1]
        minsim = 1000
        minp2 = -1
        for post2 in sample_profiles.keys():
            p2p = sample_profiles[post2]
            p_compare = p1p.compare_with(p2p)
            if p_compare.sim < minsim:
                minsim = p_compare.sim
                minp2 = post2
 
        fout.write('<a href="./%d.html">%s</a>: %s - %s<br/>\n' % (post1, nouns[post1], nouns[minp2], minsim))

    footer = """
</body></html>
"""
    fout.write(footer)    
    fout.close() 

def setup_noun_profiles(cur, tweets_nouns, post_min_freq = POST_MIN_FREQ):
    profiles_dict = get_noun_profiles(cur, post_min_freq)

    set_noun_profiles_tweet_ids(profiles_dict, tweets_nouns)

    set_noun_profiles_total(cur, profiles_dict, post_min_freq)

    set_rel_stats(profiles_dict)

    return profiles_dict

def add_none_noun(profiles_dict):
    nouns[0] = u"__никакой_пост__"
    tweets_nouns[0] = []
    profiles_dict[0] = NounProfile(0, 0, [])
    profiles_dict[0].replys_rel = synt_profile

def show_reply_freqs():
    reply_freqs = {}
    for post in profiles_dict:
        profile = profiles_dict[post]
        for reply in profile.replys:
            freq = int(math.log10(profile.replys[reply]))
            if reply not in reply_freqs:
                reply_freqs[reply] = {}
                reply_freqs[reply]["all"]= 0
            if freq not in reply_freqs[reply]:
                reply_freqs[reply][freq] = 1
            else:
                reply_freqs[reply][freq] += 1
            reply_freqs[reply]["all"] += 1

    for r in sorted(reply_freqs.keys(), key=lambda x: reply_freqs[x]["all"], reverse=True):
        print u"%s" % (nouns[r])
        for f in sorted(reply_freqs[r].keys(), reverse=True):
            print u"\t%s\t%d" % (f, reply_freqs[r][f])
    

def main():
    try:
        os.makedirs(BASE_DIR + "/profiles")
    except Exception as e:
        print e

    print "[%s] Startup " % (time.ctime())
    con = sqlite3.connect(db)
    con.isolation_level = None
    
    cur = con.cursor()
    nouns = get_nouns(cur)

    tweets_nouns = get_tweets_nouns(cur)

    profiles_dict = setup_noun_profiles(cur, tweets_nouns)
    synt_profile = get_synt_common_profile(profiles_dict)

    #for k in profiles_dict:
    #    filename  = BASE_DIR + "/profiles/" + str(k) +".json"
    #    f = open(filename, "w")
    #    json.dump(profiles_dict[k].replys_rel, f, indent=4)

    total_sims = debug_sim_net(profiles_dict, nouns, tweets_nouns)

    write_noun_sim_index(profiles_dict.keys(), nouns, total_sims)
    
    
    #target_dict = setup_noun_profiles(cur, tweets_nouns, post_min_freq = POST_MIN_FREQ / 10)

    #write_noun_sim_index2(profiles_dict, target_dict, nouns)
    print "[%s] Done " % (time.ctime())

if __name__ == '__main__':
    main()
