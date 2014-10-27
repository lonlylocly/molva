#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys
import os
import logging, logging.config
import json
from datetime import datetime, timedelta, date

import stats
from Indexer import Indexer
import util
from Fetcher import to_mysql_timestamp

logging.config.fileConfig("logging.conf")

settings = {} 
try:
    settings = json.load(open('global-settings.json', 'r'))
except Exception as e:
    logging.warn(e)

DB_DIR = settings["db_dir"] if "db_dir" in settings else os.environ["MOLVA_DIR"]

BLOCKED_NOUNS_LIST = u"\n".join(list(u"абвгдеёжзиклмнопрстуфхцчшщыьъэюя"))

BLOCKED_NOUNS = ",".join(map( lambda x: str(util.digest(x)), BLOCKED_NOUNS_LIST.split("\n")))

POST_MIN_FREQ = 10

@util.time_logger
def build_chains_nouns_replys(cur ):
    logging.info("fill replys")
    cur.execute("select count(*) from tmp.chains_nouns_all")
    logging.info("tmp.chains_nouns_all cnt = %s " % cur.fetchone()[0])
    cur.execute("""
        insert or ignore into tmp.chains_nouns_all 
        select tc.post_id, n1.noun_md5, tc.reply_id, n2.noun_md5,  ''
        from tweet_chains tc 
        inner join tweets_nouns n1 
        on n1.id = tc.post_id 
        inner join tweets_nouns n2 
        on n2.id = tc.reply_id
    """)
@util.time_logger
def build_chains_nouns_posts(cur ):
    logging.info("fill posts")
    cur.execute("""
        insert or ignore into tmp.chains_nouns_all 
        select 
            tc.post_id, 
            n1.noun_md5, 
            tc.reply_id, 
            n2.noun_md5,
            '' 
        from tweet_chains tc 
        inner join tweets_nouns n1 
        on n1.id = tc.post_id 
        inner join tweets_nouns n2 
        on n2.id = tc.post_id
        where 
            n1.noun_md5 != n2.noun_md5
    """ )

@util.time_logger
def _get_tweet_chains(cur): 
    cur.execute("select post_id, reply_id from tweet_chains")
    posts = {}

    while True:
        res = cur.fetchone()
        if res is None:
            break

        post_id, reply_id = map(int,res)
        if post_id not in posts:
            posts[post_id] = []
        posts[post_id].append(reply_id)

    return posts

@util.time_logger
def _get_tweets_nouns(cur):
    cur.execute("""
        select id, noun_md5 
        from tweets_nouns    
    """)
    
    tweets_nouns = {}
    while True:
        r = cur.fetchone()
        if r is None:
            break
        i = int(r[0])
        n = int(r[1])
        if i not in tweets_nouns:
            tweets_nouns[i] = []
        tweets_nouns[i].append(n)

    return tweets_nouns

class NounContext:

    def __init__(self,noun):
        self.noun = noun
        self.context = {}
        self.cnt = 0

    def _put_noun(self,noun):
        #if self.noun == noun:
        #    return
        if noun not in self.context:
            self.context[noun] = 0
        self.context[noun] += 1

    def put_nouns(self,nouns):
        for noun in nouns:
            self._put_noun(noun)

class NounContexts:

    def __init__(self):
        self.contexts = {}

    def put_context(self, noun, nouns):
        assert len(nouns) == len(set(nouns))
        if noun not in self.contexts:
            self.contexts[noun] = NounContext(noun)
        self.contexts[noun].put_nouns(nouns)

def get_context_nouns(noun, replys, posts, reply_nouns, post_nouns):
    context_nouns = []
    if posts:
        context_nouns += list(set(post_nouns) - set ([noun]))

    if replys:
        context_nouns += reply_nouns

    return list(set(context_nouns))

@util.time_logger
def build_contexts_in_memory(cur, cur_temp, nouns_limit, do_posts=False, do_replys=False):
    posts = _get_tweet_chains(cur)
    nouns = _get_tweets_nouns(cur)

    noun_contexts = NounContexts()

    post_id_misses = 0
    reply_id_misses = 0
    total = 0
    for post_id in posts.keys():
        if post_id not in nouns:
            post_id_misses += 1
            continue
        post_nouns = nouns[post_id]
        assert len(post_nouns) == len(set(post_nouns))
        for reply_id in posts[post_id]:
            total += 1
            if reply_id not in nouns:
                reply_id_misses += 1
                continue
            reply_nouns = nouns[reply_id]

            assert len(reply_nouns) == len(set(reply_nouns))
            for p in post_nouns:
                context_nouns = get_context_nouns(p, do_replys, do_posts, reply_nouns, post_nouns)
                noun_contexts.put_context(p, context_nouns)
        for p in post_nouns:
            if p not in noun_contexts.contexts:
                continue
            noun_contexts.contexts[p].cnt += 1

    logging.info("Total chains: %s; post id misses: %s; reply id misses: %s" %(total, post_id_misses, reply_id_misses))
    
    save_contexts(cur_temp, noun_contexts.contexts, nouns_limit)

@util.time_logger
def build_contexts_straight_in_memory(cur, cur_temp):
    nouns = _get_tweets_nouns(cur)

    noun_contexts = NounContexts()

    post_id_misses = 0
    total = 0
    for post_id in nouns.keys():
        total += 1

        post_nouns = nouns[post_id]
        for p in post_nouns:
            noun_contexts.put_context(p, post_nouns)

    logging.info("Total posts: %s" %(total))
    
    save_contexts(cur_temp, noun_contexts.contexts)

def _write_chunk(cur, table, chunk):
    cur.execute("begin transaction")
    for c in chunk:
        cur.execute("insert into %s values (%s)" % (table, ",".join(map(str,c))))
    cur.execute("commit")

@util.time_logger
def save_contexts(cur, contexts, nouns_limit):
    stats.create_given_tables(cur, ["post_cnt", "post_reply_cnt"])

    noun_cnt = []
    for noun in contexts.keys():
        noun_cnt.append((noun, contexts[noun].cnt))
    _write_chunk(cur, "post_cnt", noun_cnt) 
    print (nouns_limit)
    limit = int(1.1 * nouns_limit)
    noun_cnt = sorted(noun_cnt, key=lambda x: x[1], reverse=True)[:limit]
    logging.info("Top10 noun cnt: %s" % noun_cnt[:10])

    noun_cons = []
    total_noun_cons = 0 
    for n in noun_cnt:
        noun, cnt = n
        for context_part in contexts[noun].context.keys():
            noun_cons.append((noun, context_part, contexts[noun].context[context_part]))
            total_noun_cons += 1
        if len(noun_cons) > 20000:
            _write_chunk(cur, "post_reply_cnt", noun_cons)
            noun_cons = []

    _write_chunk(cur, "post_reply_cnt", noun_cons)

    logging.info("Nouns cnt len: %s" % len(noun_cnt))
    logging.info("Noun contexts len: %s" % total_noun_cons)

@util.time_logger
def build_chains_nouns(cur):
    cur.execute("""
        insert into chains_nouns
        select p_id, p_md5, r_id, r_md5  from chains_nouns_all
    """)

    cmd = """
        insert or ignore into post_cnt 
        select p_md5,  count(*) 
        from (
            select p_id, p_md5 
            from chains_nouns_all
            group by p_md5, p_id
        ) group by p_md5
    """ 
    logging.info(cmd)
    cur.execute(cmd)

    cur.execute("""
        insert or ignore into post_reply_cnt (post_md5, reply_md5, reply_cnt) 
        select p_md5, r_md5, count(*) 
        from chains_nouns 
        group by p_md5, r_md5;
    """)

def build_hard(cur, cur_temp, temp_f, args):
    cur.execute("attach '%s' as tmp" % temp_f)

    cur_tables = ["chains_nouns", "post_cnt", "post_reply_cnt"]
    for t in cur_tables:
        logging.info("drop %s" % t)
        cur_temp.execute("drop table if exists %s" % t)
    stats.create_given_tables(cur_temp, cur_tables)
    stats.create_given_tables(cur_temp, ["chains_nouns_all"])

    if args.reply:
        build_chains_nouns_replys(cur)
    if args.post:
        build_chains_nouns_posts(cur)

    cur.execute("detach tmp")

    build_chains_nouns(cur_temp)    

    cur_temp.execute("select count(*) from chains_nouns_all")
    logging.info("tmp.chains_nouns_all cnt = %s " % cur_temp.fetchone()[0])


def main():
    parser = util.get_dates_range_parser()
    parser.add_argument("-d", "--drop", action="store_true")
    parser.add_argument("-t", "--title")
    parser.add_argument("-p", "--post", action="store_true")
    parser.add_argument("-r", "--reply", action="store_true")
    parser.add_argument("-m", "--memory", action="store_true")
    parser.add_argument("-x", "--straight", action="store_true")
    parser.add_argument("--nouns-limit", default=2000)
    args = parser.parse_args()

    print args

    if args.title is None:
        if args.reply and args.post:
            args.title = "regular"
        elif args.reply:
            args.title = "reply"
        elif args.post:
            args.title = "post"

    nouns_limit = int(args.nouns_limit)

    output_file = "profiles/%s.nouns%s.%s.profiles.json" % (args.start, nouns_limit, args.title)
    logging.info("output file: %s" % output_file)

    temp_f = "%s/tweets_%s.db.tmp" % (DB_DIR, args.start )
    
    if args.drop and os.access(temp_f, os.R_OK):
        logging.info("Remove file %s" % temp_f)
        os.remove(temp_f)

    cur = stats.get_cursor("%s/tweets_%s.db" % (DB_DIR, args.start ))
    cur_temp = stats.get_cursor(temp_f)

    if args.drop:
        if args.memory:
            build_contexts_in_memory(cur, cur_temp, nouns_limit, do_posts=args.post, do_replys=args.reply)
        elif args.straight:
            build_contexts_straight_in_memory(cur, cur_temp)
        else:
            build_hard(cur, cur_temp, temp_f, args)

    profiles_dict = stats.setup_noun_profiles(cur_temp, {}, {}, 
        post_min_freq = POST_MIN_FREQ, blocked_nouns = BLOCKED_NOUNS, nouns_limit = nouns_limit 
    )

    logging.info("profiles len %s" % len(profiles_dict))
    profiles_dump = {}
    
    for p in sorted(profiles_dict.keys(), key=int):
        profiles_dump[p] = profiles_dict[p].replys

    json.dump(profiles_dump, open(output_file, 'w'))

if __name__ == '__main__':
    main()
