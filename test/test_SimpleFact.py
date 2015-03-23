#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys
sys.path.insert(0,'..')

import parsefacts
import xml.etree.ElementTree as ElementTree



def test_get_nouns_preps_hash1():
    root = ElementTree.parse("tomita1.xml")
    facts = []
    for el in root.findall(".//document"):
        facts += parsefacts.get_nouns_preps(el)

    is_hash_expected = [True, False, True, True, False] 
    for i in range(0, len(is_hash_expected)):
        assert facts[i].is_hash_tag == is_hash_expected[i]

    noun_expected = [u"#some1", u"солнце", u"#солнце", u"#солнцу", u"по солнцу"] 
    for i in range(0, len(noun_expected)):
        print u"Actual: %s" % unicode(facts[i].with_prep())
        print u"Expected: %s" % noun_expected[i] 
        assert facts[i].with_prep() == noun_expected[i]

    for f in facts:
        print unicode(f)

test_get_nouns_preps_hash1()
