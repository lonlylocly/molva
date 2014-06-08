#!/usr/bin/python
# -*- coding: utf-8 -*-
import hashlib
import re
import argparse
import traceback
import logging

from Exceptions import WoapeException

def digest(s):
    large = int(hashlib.md5(s.encode('utf-8')).hexdigest(), 16)

    b1 = large & (2 ** 32 - 1)
    b2 = large >> 32 & (2 ** 32 - 1)
    b3 = large >> 64 & (2 ** 32 - 1)
    b4 = large >> 96 & (2 ** 32 - 1)
    small = b1 ^ b2 ^ b3 ^ b4

    return small

def got_russian_letters(s, k=3):
    # has k or more russian letters
    res = re.match(u".*([а-яА-Я])+.*" , s) is not None
    return res

def get_dates_range_parser():
    parser = argparse.ArgumentParser()

    parser.add_argument("-s", "--start")
    parser.add_argument("-e", "--end")

    return parser

def try_several_times(f, times, error_return=[]):
    tries = 0
    while tries < times:
        try:
            tries += 1
            res = f()
            return res
        except WoapeException as e:
            logging.info("Stop trying, WoapeException: %s" % ( e))
            break
        except Exception as e:
            traceback.print_exc()
            logging.error(e)

    return error_return


