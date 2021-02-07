import os
import sys

import hail as hl
from hail.utils.java import Env

import varspark as vs


def init(quiet=False, spark_conf=None, **kwargs):
    """ Initialises hail context with variant-spark support.

        :param kwargs: same as for hail.init()
    """

    jars = [p.strip() for p in spark_conf.get("spark.jars", "").split(",")] if spark_conf else []
    vs_jar_path = vs.find_jar()
    assert os.path.exists(vs_jar_path), "%s does not exist" % vs_jar_path
    if not quiet:
        sys.stderr.write("using variant-spark jar at '%s'\n" % vs_jar_path)
    if not vs_jar_path in jars:
        jars.append(vs_jar_path)
    hl.init(quiet=quiet, spark_conf={'spark.jars': ",".join(jars)}, **kwargs)


def version():
    return Env.jvm().au.csiro.variantspark.python.Metadata.version()


def version_info():
    return Env.jvm().au.csiro.variantspark.python.Metadata.gitProperties()
