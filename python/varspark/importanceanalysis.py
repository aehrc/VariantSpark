import pandas as pd
from typedecorator import params, Nullable

class ImportanceAnalysis(object):
    def __init__(self, _jia, sql):
        self._jia = _jia
        self.sql = sql

    @params(self=object, limit=Nullable(int), normalized=Nullable(bool))
    def important_variables(self, limit=10, normalized=False):
        """ Gets the top limit important variables

        :param (int) limit: Indicates how many of the most important variables to return
        :param (bool) normalized: Indicates whether to return normalized importances

        :return topimportances (pd.DataFrame): Dataframe of most important variables containing a
            variant_id and its corresponding importance.
        """
        jimpvarmap = self._jia.importantVariablesJavaMap(limit, normalized)
        jimpvarmapsorted = sorted(jimpvarmap.items(), key=lambda x: x[1], reverse=True)
        topimportances = pd.DataFrame(jimpvarmapsorted, columns=['variable', 'importance'])
        return topimportances

    @params(self=object, precision=Nullable(int), normalized=Nullable(bool))
    def variable_importance(self, precision=None, normalized=False):
        """ Returns a DataFrame with the gini importance of variables.

        :param (int) precision: Maximum floating point precision to return
        :param (bool) normalized: Indicates whether to return normalized importances

        :return importances (pd.DataFrame): DataFrame of variable importances containing variant_id, importance, and split count
        """
        jdf = self._jia.variableImportance(normalized)
        jdf.count()
        jdf.createOrReplaceTempView("df")
        importances = self.sql.table("df").toPandas()
        if precision is not None:
            importances['importance'] = importances['importance'].apply(lambda x: round(x, precision))
        return importances
