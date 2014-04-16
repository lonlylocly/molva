#!/bin/bash

tweets_text=$1
facts=$2

cat $tweets_text | ./tomita-linux64 tomita/config.proto > $facts

