#!/usr/bin/python
# -*- coding: utf-8 -*-
import hashlib

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


