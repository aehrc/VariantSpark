# Ensure backwards compatibility with Python 2
from __future__ import (
    absolute_import,
    division,
    print_function)

from varspark import find_jar

def cli():
    print(find_jar())
