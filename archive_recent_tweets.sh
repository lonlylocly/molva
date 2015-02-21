#!/bin/bash -ex
date_2=$(date -d 'now - 2 day' +'%Y%m%d')
date_4=$(date -d 'now - 4 day' +'%Y%m%d')
date_6=$(date -d 'now - 6 day' +'%Y%m%d')

old_db="/home/lonlylocly/streaming/tweets_$date_4.db.gz"
new_db="/home/lonlylocly/streaming/tweets_$date_2.db"

old_words="/home/lonlylocly/streaming/words_$date_6.db.gz"
new_words="/home/lonlylocly/streaming/words_$date_4.db"

# release db descriptors; sort of 'magic'
pkill woape-statuses -f || echo "No such process"

if [ -e "$old_db" ] ; then
    rm "$old_db" 
fi

if [ -e "$old_words" ] ; then
    rm "$old_words" 
fi

if [ -e "$new_db" ] ; then
    gzip "$new_db"
fi

if [ -e "$new_words" ] ; then
    gzip "$new_words" 
fi
