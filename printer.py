#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys,codecs
import json

sys.stdout = codecs.getwriter('utf8')(sys.stdout)

f = sys.argv[1]

print json.dumps(json.load(open(f, 'r')), indent=4, ensure_ascii=False)
