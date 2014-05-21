woape.py
    produces:
        /tweets.db - users
        /tweets_<DATE>.db - tweets

pre-tomita.py
    depends
        /tweets_<DATE>.db - tweets
    produces
        /index/<DATE>_<CHUNK_ID>.index.txt
        /index/<DATE>_<CHUNK_ID>.tweets.txt

run-tomita.py
    depends
        /index/<DATE>_<CHUNK_ID>.index.txt
        /index/<DATE>_<CHUNK_ID>.tweets.txt (removes)
    produces
        /nouns/<DATE>_<CHUNK_ID>.facts.xml

parsefacts.py
    depends
        /index/<DATE>_<CHUNK_ID>.index.txt (removes)
        /nouns/<DATE>_<CHUNK_ID>.facts.xml (removes)
    produces
        /tweets_<DATE>.db - nouns 
        /tweets_<DATE>.db - tweet_nouns 

post-tomita.py
    depends
        /tweets_<DATE>.db - tweets
        /tweets_<DATE>.db - tweet_nouns 
    produces
        /tweets_<DATE>.db - tweet_chains, chains_nouns, post_reply_cnt, post_cnt 

build-profiles.py
    depends 
        /tweets_<DATE>.db - post_reply_cnt, post_cnt 
    produces
        /tweets_<DATE>.db - noun_similarity 

build-clusters.py
    depends 
        /tweets_<DATE>.db - noun_similarity
    produces
        /tweets.db - clusters

handler.py
    depends
        /tweets.db
            clusters
            tweets
        /tweets_<DATE>.db
            nouns
            post_cnt
            post_reply_cnt
            noun_similarity

