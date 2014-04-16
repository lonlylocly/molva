#!/usr/bin/python
import stats
import sys
import time
import os

db = os.environ["MOLVA_DB"]


def main():
    cur = stats.get_cursor(db)
    
    #stats.create_tables(cur)
    #stats.fill_tweet_chains(cur)

    index_file = open(sys.argv[1] + ".index.txt", "w")
    tweets_file = open(sys.argv[1] + ".tweets.txt", "w")

    tweets = cur.execute("""
        select id, tw_text 
        from tweets
    """)
    
    t = tweets.fetchone()
    cnt = 0
    while t is not None:
        index_file.write("%d\n" % t[0])
        cnt = cnt + 1
        tw_text = t[1]
        tw_text = tw_text.encode('utf-8')
        tw_text = tw_text.replace('\n', ' ').replace("'", "\\'")
        tweets_file.write("%s\n" % tw_text)
        t = tweets.fetchone()
        if cnt % 100000 == 0:
            print "[%s] %s tweets done" % (time.ctime(), cnt)

if __name__ == '__main__':
    main()
