import glob
import os
import pkg_resources

def find_jar():
    jars_dir = pkg_resources.resource_filename(__name__, "jars")
    return glob.glob(os.path.join(jars_dir, "*-all.jar"))[0]
