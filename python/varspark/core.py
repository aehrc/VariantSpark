# Ensure backwards compatibility with Python 2
from __future__ import (
    absolute_import,
    division,
    print_function)

import sys
from random import randint
from typedecorator import params, Nullable, Union, setup_typecheck
from pyspark import SparkConf
from pyspark.sql import SQLContext
from varspark.etc import find_jar
from varspark import java

if sys.version_info > (3,):
    long = int


class VarsparkContext(object):
    """The main entry point for VariantSpark functionality.
    """

    @classmethod
    def spark_conf(cls, conf  = SparkConf()):
        """ Adds the necessary option to the spark configuration.
        Note: In client mode these need to be setup up using --jars or --driver-class-path
        """
        return conf.set("spark.jars", find_jar())

    def __init__(self, ss, silent = False):
        """The main entry point for VariantSpark functionality.
        :param ss: SparkSession
        :type ss: :class:`.pyspark.SparkSession`
        :param bool silent: Do not produce welcome info.
        """
        self.sc = ss.sparkContext
        self.silent = silent
        self.sql = SQLContext.getOrCreate(self.sc)
        self._jsql = self.sql._jsqlContext
        self._jvm = self.sc._jvm
        self._vs_api = getattr(self._jvm, 'au.csiro.variantspark.api')
        jss = ss._jsparkSession
        self._jvsc = self._vs_api.VSContext.apply(jss)

        setup_typecheck()

        if not self.silent:
            sys.stderr.write('Running on Apache Spark version {}\n'.format(self.sc.version))
            if self.sc._jsc.sc().uiWebUrl().isDefined():
                sys.stderr.write('SparkUI available at {}\n'.format(
                        self.sc._jsc.sc().uiWebUrl().get()))
            sys.stderr.write(
                'Welcome to\n'
                ' _    __           _             __  _____                  __    \n'
                '| |  / /___ ______(_)___ _____  / /_/ ___/____  ____ ______/ /__  \n'
                '| | / / __ `/ ___/ / __ `/ __ \/ __/\__ \/ __ \/ __ `/ ___/ //_/  \n'
                '| |/ / /_/ / /  / / /_/ / / / / /_ ___/ / /_/ / /_/ / /  / ,<     \n'
                '|___/\__,_/_/  /_/\__,_/_/ /_/\__//____/ .___/\__,_/_/  /_/|_|    \n'
                '                                      /_/                         \n')

    @params(self=object, vcf_file_path=str, min_partitions=int)
    def import_vcf(self, vcf_file_path, min_partitions=0):
        """ Import features from a VCF file.
        """
        return FeatureSource(self._jvm, self._vs_api,
                            self._jsql, self.sql, self._jvsc.importVCF(vcf_file_path,
                            min_partitions))

    @params(self=object, label_file_path=str, col_name=str)
    def load_label(self, label_file_path, col_name):
        """ Loads the label source file

        :param label_file_path: The file path for the label source file
        :param col_name: the name of the column containing labels
        """
        return self._jvsc.loadLabel(label_file_path, col_name)

    def stop(self):
        """ Shut down the VariantsContext.
        """

        self.sc.stop()
        self.sc = None

# Deprecated
VariantsContext = VarsparkContext


class FeatureSource(object):

    def __init__(self, _jvm, _vs_api, _jsql, sql, _jfs):
        self._jfs = _jfs
        self._jvm = _jvm
        self._vs_api = _vs_api
        self._jsql = _jsql
        self.sql = sql

    @params(self=object, label_source=object, n_trees=Nullable(int), mtry_fraction=Nullable(float),
            oob=Nullable(bool), seed=Nullable(Union(int, long)), batch_size=Nullable(int),
            var_ordinal_levels=Nullable(int))
    def importance_analysis(self, label_source, n_trees=1000, mtry_fraction=None,
                            oob=True, seed=None, batch_size=100, var_ordinal_levels=3):
        """Builds random forest classifier.

        :param label_source: The ingested label source
        :param int n_trees: The number of trees to build in the forest.
        :param float mtry_fraction: The fraction of variables to try at each split.
        :param bool oob: Should OOB error be calculated.
        :param long seed: Random seed to use.
        :param int batch_size: The number of trees to build in one batch.
        :param int var_ordinal_levels:

        :return: Importance analysis model.
        :rtype: :py:class:`ImportanceAnalysis`
        """
        jrf_params = self._jvm.au.csiro.variantspark.algo.RandomForestParams(bool(oob),
                                java.jfloat_or(mtry_fraction), True, java.NAN, True,
                                java.jlong_or(seed, randint(java.MIN_LONG, java.MAX_LONG)))
        jia = self._vs_api.ImportanceAnalysis(self._jsql, self._jfs, label_source,
                                              jrf_params, n_trees, batch_size, var_ordinal_levels)
        return ImportanceAnalysis(jia, self.sql)


class ImportanceAnalysis(object):
    """ Model for random forest based importance analysis
    """

    def __init__(self, _jia, sql):
        self._jia = _jia
        self.sql = sql

    @params(self=object, limit=Nullable(int))
    def important_variables(self, limit=10):
        """ Gets the top limit important variables as a list of tuples (name, importance) where:
            - name: string - variable name
            - importance: double - gini importance
        """
        jimpvarmap = self._jia.importantVariablesJavaMap(limit)
        return sorted(jimpvarmap.items(), key=lambda x: x[1], reverse=True)

    def oob_error(self):
        """ OOB (Out of Bag) error estimate for the model

        :rtype: float
        """
        return self._jia.oobError()

    def variable_importance(self):
        """ Returns a DataFrame with the gini importance of variables.

        The DataFrame has two columns:
        - variable: string - variable name
        - importance: double - gini importance
        """
        jdf = self._jia.variableImportance()
        jdf.count()
        jdf.createTempView("df")
        return self.sql.table("df")
