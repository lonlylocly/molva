#!/bin/bash

dir_in=$1
dir_out=$2
mkdir -p $dir_out

for i in $(ls $dir_in/index/*tweets*) ; do
    
cat $tweets_text | ./tomita-linux64 tomita/config.proto > $facts
done

