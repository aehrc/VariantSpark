import os
import sys

import pkg_resources
import pyspark
from hail.backend.spark_backend import install_exception_handler, connect_logger, \
    __name__ as __hail_name__
from hail.utils.java import scala_package_object


#
# This is a customised version of the constructor honoring spark.jars passed in
# spark_config
#
# pylint: disable=C0415

def SparkBackend__init__(self, idempotent, sc, spark_conf, app_name, master,
                         local, log, quiet, append, min_block_size,
                         branching_factor, tmpdir, local_tmpdir, skip_logging_configuration,
                         optimizer_iterations):
    if pkg_resources.resource_exists(__hail_name__, "hail-all-spark.jar"):
        hail_jar_path = pkg_resources.resource_filename(__hail_name__, "hail-all-spark.jar")
        assert os.path.exists(hail_jar_path), f'{hail_jar_path} does not exist'
        conf = pyspark.SparkConf()

        base_conf = spark_conf or {}
        for k, v in base_conf.items():
            conf.set(k, v)

        # Initialize jars from the current configuraiton
        jars = [p.strip() for p in conf.get("spark.jars", "").split(",")]
        if not hail_jar_path in jars:
            jars.insert(0, hail_jar_path)

        if os.environ.get('HAIL_SPARK_MONITOR'):
            import sparkmonitor
            jars.append(os.path.join(os.path.dirname(sparkmonitor.__file__), 'listener.jar'))
            conf.set("spark.extraListeners", "sparkmonitor.listener.JupyterSparkMonitorListener")

        conf.set('spark.jars', ','.join(jars))
        conf.set('spark.driver.extraClassPath', ','.join(jars))
        conf.set('spark.executor.extraClassPath', './hail-all-spark.jar')
        if sc is None:
            pyspark.SparkContext._ensure_initialized(conf=conf)
        elif not quiet:
            sys.stderr.write(
                'pip-installed Hail requires additional configuration options in Spark referring\n'
                '  to the path to the Hail Python module directory HAIL_DIR,\n'
                '  e.g. /path/to/python/site-packages/hail:\n'
                '    spark.jars=HAIL_DIR/hail-all-spark.jar\n'
                '    spark.driver.extraClassPath=HAIL_DIR/hail-all-spark.jar\n'
                '    spark.executor.extraClassPath=./hail-all-spark.jar')
    else:
        pyspark.SparkContext._ensure_initialized()

    self._gateway = pyspark.SparkContext._gateway
    self._jvm = pyspark.SparkContext._jvm

    hail_package = getattr(self._jvm, 'is').hail

    self._hail_package = hail_package
    self._utils_package_object = scala_package_object(hail_package.utils)

    jsc = sc._jsc.sc() if sc else None

    if idempotent:
        self._jbackend = hail_package.backend.spark.SparkBackend.getOrCreate(
            jsc, app_name, master, local, True, min_block_size, tmpdir, local_tmpdir)
        self._jhc = hail_package.HailContext.getOrCreate(
            self._jbackend, log, True, append, branching_factor, skip_logging_configuration,
            optimizer_iterations)
    else:
        self._jbackend = hail_package.backend.spark.SparkBackend.apply(
            jsc, app_name, master, local, True, min_block_size, tmpdir, local_tmpdir)
        self._jhc = hail_package.HailContext.apply(
            self._jbackend, log, True, append, branching_factor, skip_logging_configuration,
            optimizer_iterations)

    self._jsc = self._jbackend.sc()
    if sc:
        self.sc = sc
    else:
        self.sc = pyspark.SparkContext(gateway=self._gateway,
                                       jsc=self._jvm.JavaSparkContext(self._jsc))
    self._jspark_session = self._jbackend.sparkSession()
    self._spark_session = pyspark.sql.SparkSession(self.sc, self._jspark_session)

    # This has to go after creating the SparkSession. Unclear why.
    # Maybe it does its own patch?
    install_exception_handler()

    from hail.context import version

    py_version = version()
    jar_version = self._jhc.version()
    if jar_version != py_version:
        raise RuntimeError(f"Hail version mismatch between JAR and Python library\n"
                           f"  JAR:    {jar_version}\n"
                           f"  Python: {py_version}")

    self._fs = None
    self._logger = None

    if not quiet:
        sys.stderr.write('Running on Apache Spark version {}\n'.format(self.sc.version))
        if self._jsc.uiWebUrl().isDefined():
            sys.stderr.write('SparkUI available at {}\n'.format(self._jsc.uiWebUrl().get()))

        connect_logger(self._utils_package_object, 'localhost', 12888)

        self._jbackend.startProgressBar()
