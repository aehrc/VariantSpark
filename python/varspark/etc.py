# Ensure backwards compatibility with Python 2
from __future__ import (
    absolute_import,
    division,
    print_function)

import glob
import os
import pkg_resources

def find_jar():
    """Gets the path to the variant spark jar bundled with the
    python distribution
    """
    # try to find the distribution jars first
    jars_dir = pkg_resources.resource_filename(__name__, "jars")
    if not os.path.isdir(jars_dir):
        #then it can be an develoment install
        jars_dir = os.path.abspath(pkg_resources.resource_filename(__name__,
                                            os.path.join(os.pardir, os.pardir, "target")))
    return glob.glob(os.path.join(jars_dir, "*-all.jar"))[0]
