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

logging.config.fileConfig("logging.conf")

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
        os.rename(tweets_file, os.path.join(in_dir, tag + ".done.txt"))
        
if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2])

