'''
Created on 20 Jun 2018

@author: szu004
'''

# Ensure backwards compatibility with Python 2
from __future__ import (
    absolute_import,
    division,
    print_function)

import sys
if sys.version_info > (3,):
    long = int

MAX_LONG =  long(9223372036854775807)
MIN_LONG = long(-9223372036854775808)

NAN = float('nan')

def jtype_or(t, v, def_v):
    return t(def_v) if v is None else t(v)

def jfloat_or(v, def_v = NAN):
    return jtype_or(float, v, def_v)

def jlong_or (v, def_v ):
    return jtype_or(long, v, def_v)
