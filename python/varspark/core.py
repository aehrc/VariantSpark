import sys

from pyspark import SparkConf
from pyspark.sql import SQLContext
from typedecorator import params, Nullable, setup_typecheck

from varspark import java
from varspark.etc import find_jar
from varspark.featuresource import FeatureSource


class VarsparkContext(object):
    """The main entry point for VariantSpark functionality."""

    @classmethod
    def spark_conf(cls, conf=SparkConf()):
        """Adds the necessary option to the spark configuration.
        Note: In client mode these need to be setup up using --jars or --driver-class-path
        """
        return conf.set("spark.jars", find_jar())

    def __init__(self, ss, silent=False):
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
        self._vs_api = getattr(self._jvm, "au.csiro.variantspark.api")
        jss = ss._jsparkSession
        self._jvsc = self._vs_api.VSContext.apply(jss)

        setup_typecheck()

        if not self.silent:
            sys.stderr.write(
                "Running on Apache Spark version {}\n".format(self.sc.version)
            )
            if self.sc._jsc.sc().uiWebUrl().isDefined():
                sys.stderr.write(
                    "SparkUI available at {}\n".format(
                        self.sc._jsc.sc().uiWebUrl().get()
                    )
                )
            sys.stderr.write(
                "Welcome to\n"
                " _    __           _             __  _____                  __    \n"
                "| |  / /___ ______(_)___ _____  / /_/ ___/____  ____ ______/ /__  \n"
                "| | / / __ `/ ___/ / __ `/ __ \/ __/\__ \/ __ \/ __ `/ ___/ //_/  \n"
                "| |/ / /_/ / /  / / /_/ / / / / /_ ___/ / /_/ / /_/ / /  / ,<     \n"
                "|___/\__,_/_/  /_/\__,_/_/ /_/\__//____/ .___/\__,_/_/  /_/|_|    \n"
                "                                      /_/                         \n"
            )

    @params(self=object, vcf_file_path=str, imputation_strategy=Nullable(str))
    def import_vcf(self, vcf_file_path, imputation_strategy="none"):
        """Import features from a VCF file.
        
        :param vcf_file_path String: The file path for the vcf file to import
        :param imputation_strategy String:
            The imputation strategy to use. Options for imputation include:

            - none: No imputation will be performed. Missing values will be replaced with -1 (not recommended unless there are no missing values)
            - mode: Missing values will be replaced with the most commonly occuring value among that feature. Recommended option
            - zeros: Missing values will be replaced with zeros. Faster than mode imputation
        """
        if imputation_strategy == "none":
            print("WARNING: Imputation strategy is set to none - please ensure that there are no missing values in the data.")
        return FeatureSource(
            self._jvm,
            self._vs_api,
            self._jsql,
            self.sql,
            self._jvsc.importVCF(vcf_file_path, imputation_strategy),
        )

    @params(
        self=object,
        cov_file_path=str,
        cov_types=Nullable(dict),
        transposed=Nullable(bool),
    )
    def import_covariates(self, cov_file_path, cov_types=None, transposed=False):
        """Import covariates from a CSV file.

        :param cov_file_path String: The file path for covariate csv file
        :param cov_types Dict[String]:
            A dictionary specifying types for each covariate, where the key is the variable name
            and the value is the type. The value can be one of the following:

            - CONTINUOUS: A continuous variable type.
            - DISCRETE: A discrete variable type.
            - NOMINAL: A nominal variable type.
            - ORDINAL: An ordinal variable type.
            - ORDINAL(order_count): Specifies the number of ordered levels, where `order_count` represents the number of levels.
            - NOMINAL(class_count): Specifies the number of distinct classes, where `class_count` represents the number of categories.

            See VariableType.scala for more information.
        :param transposed bool: Whether or not the covariate csv file is transposed
        """
        if cov_types is not None:
            cov_types_list = [f"{k},{c}" for k, c in cov_types.items()]
            _jctypes = self._jvm.java.util.ArrayList()
            for item in cov_types_list:
                _jctypes.add(item)
        else:
            _jctypes = None
        if transposed:
            _jcs = self._jvsc.importTransposedCSV(cov_file_path, cov_types_list)
        else:
            _jcs = self._jvsc.importStdCSV(cov_file_path, cov_types_list)
        return FeatureSource(
            self._jvm,
            self._vs_api,
            self._jsql,
            self.sql,
            _jcs,
        )

    @params(self=object, feature_source=FeatureSource, covariate_source=FeatureSource)
    def union_features_and_covariates(self, feature_source, covariate_source):
        return FeatureSource(
            self._jvm,
            self._vs_api,
            self._jsql,
            self.sql,
            self._jvsc.unionFeaturesAndCovariates(
                feature_source._jfs, covariate_source._jfs
            ),
        )

    @params(self=object, label_file_path=str, col_name=str)
    def load_label(self, label_file_path, col_name):
        """Loads the label source file

        :param label_file_path: The file path for the label source file
        :param col_name: the name of the column containing labels
        """
        return self._jvsc.loadLabel(label_file_path, col_name)

    def stop(self):
        """Shut down the VariantsContext."""

        self.sc.stop()
        self.sc = None


# Deprecated
VariantsContext = VarsparkContext
