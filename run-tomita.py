#!/usr/bin/python
import sys
#import time
import os
import os.path
import logging, logging.config
import re
#import codecs
from subprocess import call 
import subprocess
import json

logging.config.fileConfig("logging.conf")

settings = {} 
try:
    settings = json.load(open('global-settings.json', 'r'))
except Exception as e:
    logging.warn(e)

DB_DIR = settings["db_dir"] if "db_dir" in settings else os.environ["MOLVA_DIR"]

def main(in_dir, out_dir):
    log = logging.getLogger('tomiter')
    try:
        os.makedirs(out_dir)
    except Exception as e:
        log.warn(e)

    files = {}
    for f in os.listdir(in_dir):
        full_path = os.path.join(in_dir, f)
        if os.path.isfile(full_path):
            match = re.search("(.*)\.tweets\.txt", f)
            if match:
                files[match.group(1)] = full_path

    log.info("Got %s new files" % len(files))    
    for tag in sorted(files.keys()):
        tweets_file = files[tag]
        facts_file = os.path.join(out_dir, tag + ".facts.xml")
        cmd = "cat " + tweets_file + " | ./tomita-linux64 tomita/config.proto > " + facts_file
        log.info(cmd)
        ret = call(cmd, stderr=subprocess.STDOUT, shell=True) 
        if ret != 0:
            raise Exception("Failed to exec %s: exit code %s" % (cmd, ret))
        os.remove(tweets_file)
    
    log.info("Done")
        
if __name__ == '__main__':
    main(DB_DIR + "/index/", DB_DIR + "/nouns/")

