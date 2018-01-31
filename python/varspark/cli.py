# Ensure backwards compatibility with Python 2
from __future__ import (
    absolute_import,
    division,
    print_function)

import subprocess
import sys
from varspark import find_jar

def varspark_jar():
    print(find_jar())

def varspark_submit():
    args = ['spark-submit', '--jars', find_jar()] + sys.argv[1:]
    exit(subprocess.call(" ".join(["'%s'"%arg for arg in args]), shell=True))
