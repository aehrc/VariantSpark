import struct
import pandas as pd
from typedecorator import params, Nullable


class FeatureSource(object):
    def __init__(self, _jvm, _vs_api, _jsql, sql, _jfs):
        self._jfs = _jfs
        self._jvm = _jvm
        self._vs_api = _vs_api
        self._jsql = _jsql
        self.sql = sql

    @params(self=object, row_lim=Nullable(int), col_lim=Nullable(int))
    def head(self, row_lim=10, col_lim=10):
        """Converts a Feature Source RDD to a pandas dataframe.

        :param (int) row_lim: Specifies the number of rows (features) to take
        :param (int) col_lim: Specifies the number of columns (samples) to take

        :return features (DataFrame): dataframe with values for respective samples (rows)
        """
        jdf = self._jfs.head(self._jsql, row_lim, col_lim)
        jdf.count()
        jdf.createOrReplaceTempView("df")
        features = self.sql.table("df").toPandas()
        features.set_index("variant_id", inplace=True)
        return features
