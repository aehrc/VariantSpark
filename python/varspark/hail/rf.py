from hail.ir import *
from hail.table import Table
from hail.typecheck import *
from hail.utils.java import Env

#from varspark.pvalues_calculation import *
import hail as hl


class RandomForestModel(object):
    """ Represents a random forest model object. Do not construct it directly but rather
            use `varspark.hail.methods.random_forest_model`  function.
    """

    @typecheck_method(
        _mir=MatrixIR,
        oob=bool,
        mtry_fraction=nullable(float),
        min_node_size=nullable(int),
        max_depth=nullable(int),
        seed=nullable(int),
        imputation_type=nullable(str)
    )
    def __init__(self, _mir, oob=True, mtry_fraction=None, min_node_size=None,
                 max_depth=None, seed=None, imputation_type=None):
        self._mir = _mir
        self._jrf_model = Env.backend().jvm().au.csiro.variantspark.hail.methods.RFModel.pyApply(
            Env.backend()._jbackend,
            Env.spark_backend('rf')._to_java_matrix_ir(self._mir),
            mtry_fraction, oob, min_node_size,
            max_depth, seed, imputation_type)

    def __enter__(self):
        return self

    def __exit__(self, *exc_details):
        self.release()

    @typecheck_method(
        n_trees=int,
        batch_size=int
    )
    def fit_trees(self, n_trees=500, batch_size=100):
        """ Fits the random forest model.

            :param int n_trees: The number of trees to build in the forest.
            :param int batch_size: The number of trees to build in one batch.
        """

        self._jrf_model.fitTrees(n_trees, batch_size)

    def oob_error(self):
        """ Returns the Out of Bag (OOB) error for this model. Only available if the model was created with the
            `oob` option.

            :rtype: float
        """
        return self._jrf_model.oobError()

    def variable_importance(self):
        """ Returns the variable importance for this model in a hail `Table` with the following row fields:

                'locus': locus<GRCh37>
                'alleles': array<str>
                'importance': float64

            and indexed with: ['locus', 'alleles'].

            The `importance` column contains gini importance for each of the variants.

            :rtype: :py:class:`hail.is.Table`
        """
        return Table._from_java(self._jrf_model.variableImportance())

    @typecheck_method(
        split_count_threshold=int,
        pvalue=float
    )
    def get_significant_variances(self, split_count_threshold=1, pvalue=0.05):
        """ Returns a dataframe with the significant variants and their p-value

        :param split_count_threshold: Determine the cutoff (of how many times a variable was used to split a tree) to get a unimodal density
        :param pvalue: Threshold value for the p-value (typically 0.05)
        :return: dataframe with variants and their significance
        """

        impTable = self.variable_importance()
        df = impTable.order_by(hl.desc(impTable.importance)).to_spark(flatten=False).toPandas()
        df['log_importance'] = df.importance.apply(np.log)
        df = df[(df.importance > 0) & (df['splitCount'] > split_count_threshold)]
        df['composed_index'] = df.apply(
            lambda row: str(row['locus'][0]) + '_' + str(row['locus'][1]) + '_' + str(
                '_'.join(row['alleles'])), axis=1)  # .tolist()
        df = df[['composed_index', 'log_importance']].set_index('composed_index').squeeze()
        temp = run_it_importances(df, pvalue=pvalue)
        dfr = df.head(len(temp['ppp'])).reset_index()
        dfr['pvals'] = temp['ppp']
        return dfr

    @typecheck_method(
        filename=str,
        resolve_names=bool
    )
    def to_json(self, filename, resolve_names=True):
        """ Saves the model JSON representation to a file. If `resolve_names` is set
            includes the variable names as well as indexes in the output. This does however
            incur performance penalty for creation of in-memory variable index.

            :param str filename: The file to save the model to.
            :param bool resolve_names: Resolve variable names in the saved JSON.
        """
        self._jrf_model.toJson(filename, resolve_names)

    def release(self):
        """ Release all in-memory resources associated with this model.
            This may include cached datasets, etc.
        """
        self._jrf_model.release()
