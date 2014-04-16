#!/usr/bin/python
# -*- coding: utf-8 -*-
import sqlite3
from subprocess import Popen, PIPE, STDOUT
import sys,codecs
import re
import hashlib
import time
import json
import random

import simdict
from util import digest

sys.stdout = codecs.getwriter('utf8')(sys.stdout)

s = u"дак"
print s

f = open("enc.txt", "w")
f.write(s.encode('utf8'))
f.close()
