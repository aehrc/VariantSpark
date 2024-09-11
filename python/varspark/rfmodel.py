from random import randint

from pyspark.sql import SparkSession, SQLContext
from typedecorator import params, Nullable

from varspark import java
from varspark.importanceanalysis import ImportanceAnalysis
from varspark.lfdrvsnohail import LocalFdrVs

class RandomForestModel(object):
    @params(self=object, ss=SparkSession,
                 mtry_fraction=Nullable(float), oob=Nullable(bool),
                 seed=Nullable(int), var_ordinal_levels=Nullable(int),
                 max_depth=int, min_node_size=int)
    def __init__(self, ss, mtry_fraction=None,
                 oob=True, seed=None, var_ordinal_levels=3,
                 max_depth=java.MAX_INT, min_node_size=1):
        self.ss = ss
        self.sc = ss.sparkContext
        self._jvm = self.sc._jvm
        self._vs_api = getattr(self._jvm, 'au.csiro.variantspark.api')
        self.sql = SQLContext.getOrCreate(self.sc)
        self._jsql = self.sql._jsqlContext
        self.mtry_fraction=mtry_fraction
        self.oob = oob
        self.seed = seed
        self.var_ordinal_levels = var_ordinal_levels
        self.max_depth = max_depth
        self.min_node_size = min_node_size
        self.vs_algo = self._jvm.au.csiro.variantspark.algo
        self.jrf_params = self.vs_algo.RandomForestParams(bool(oob),
                                                java.jfloat_or(
                                                    mtry_fraction),
                                                True, java.NAN, True,
                                                java.jlong_or(seed,
                                                              randint(
                                                                  java.MIN_LONG,
                                                                  java.MAX_LONG)),
                                                max_depth,
                                                min_node_size, False,
                                                0)
        self._jrf_model = None

    @params(self=object, X=object, y=object, n_trees=Nullable(int), batch_size=Nullable(int))
    def fit_trees(self, X, y, n_trees=1000, batch_size=100):
        """ Fits random forest model on provided input features and labels
        :param (int) n_trees: Number of trees in the forest
        :param (int) batch_size:
        """
        self.n_trees = n_trees
        self.batch_size = batch_size
        self._jfs = X._jfs
        self._jrf_model = self._vs_api.RFModelTrainer.trainModel(X._jfs, y, self.jrf_params, self.n_trees, self.batch_size)

    @params(self=object)
    def importance_analysis(self):
        """ Returns gini variable importances for a fitted random forest model
        :return ImportanceAnalysis: Class containing importances and associated methods
        """
        jia = self._vs_api.ImportanceAnalysis(self._jsql, self._jfs, self._jrf_model)
        return ImportanceAnalysis(jia, self.sql)

    @params(self=object)
    def oob_error(self):
        """ Returns the overall out-of-bag error associated with a fitted random forest model
        :return oob_error (float): Out of bag error associated with the fitted model
        """
        oob_error = self._jrf_model.oobError()
        return oob_error

    @params(self=object)
    def get_lfdr(self):
        """ Returns the class with the information preloaded to compute the local FDR
        :return: class LocalFdrVs with the importances loaded
        """
        return LocalFdrVs.from_imp_df(self.importance_analysis().variable_importance())

    @params(self=object, file_name=str, resolve_variable_names=Nullable(bool), batch_size=Nullable(int))
    def export_to_json(self, file_name, resolve_variable_names=True, batch_size=1000):
        """ Exports the random forest model to Json format

        :param (string) file_name: File name to export
        :param (bool) resolve_variable_names: Indicates whether to associate variant ids with exported nodes
        :param (int) batch_size: Number of trees to process in a single batch during export
        """
        jexp = self._vs_api.ExportModel(self._jrf_model, self._jfs)
        jexp.toJson(file_name, resolve_variable_names, batch_size)

# Deprecated
RFModelContext = RandomForestModel
