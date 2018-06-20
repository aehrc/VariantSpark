# Ensure backwards compatibility with Python 2
from __future__ import (
    absolute_import,
    division,
    print_function)

import os
import glob

THIS_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.abspath(os.path.join(THIS_DIR, os.pardir, os.pardir, os.pardir))

def find_variants_jar():
    jar_candidates = glob.glob(os.path.join(PROJECT_DIR, 'target','variant-spark_*-all.jar'))
    assert len(jar_candidates) == 1, "Expecting one jar, but found: %s" % str(jar_candidates)
    return jar_candidates[0]
