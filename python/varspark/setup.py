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
    jars_dir = pkg_resources.resource_filename(__name__, "jars")
    return glob.glob(os.path.join(jars_dir, "*-all.jar"))[0]
