import os
import sys

import hail as hl
import pkg_resources
from hail.utils.java import Env
from pyspark import SparkConf, SparkContext

import varspark as vs

def init(**kwargs):
    """ Initialises hail context with variant-spark support.

        :param kwargs: same as for hail.init()
    """

    jars = []
    vs_jar_path = vs.find_jar()
    assert os.path.exists(vs_jar_path), "%s does not exist" % vs_jar_path
    sys.stderr.write("using variant-spark jar at '%s'\n" % vs_jar_path)
    jars.append(vs_jar_path)

    if pkg_resources.resource_exists(hl.__name__, "hail-all-spark.jar"):
        hail_jar_path = pkg_resources.resource_filename(hl.__name__, "hail-all-spark.jar")
        assert os.path.exists(hail_jar_path), "%s does not exist" % hail_jar_path
        sys.stderr.write("using hail jar at '%s'\n" % hail_jar_path)
        jars.append(hail_jar_path)

    conf = SparkConf()
    conf.set('spark.jars', ",".join(jars))
    conf.set('spark.driver.extraClassPath', hail_jar_path)
    conf.set('spark.executor.extraClassPath', './hail-all-spark.jar')
    SparkContext._ensure_initialized(conf=conf)

    hl.init(**kwargs)


def version():
    return Env.jvm().au.csiro.variantspark.python.Metadata.version()


def version_info():
    return Env.jvm().au.csiro.variantspark.python.Metadata.gitProperties()
