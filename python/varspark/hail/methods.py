from typing import Dict
from hail.utils import wrap_to_list
from hail.utils.java import Env


from hail.expr.expressions import *
from hail.typecheck import *
from hail.methods.statgen import _get_regression_row_fields

from . import rf


@typecheck(
    y=expr_float64,
    x=expr_int32,
    covariates=dictof(str, expr_float64),
    oob=bool,
    mtry_fraction=nullable(float),
    min_node_size=nullable(int),
    max_depth=nullable(int),
    seed=nullable(int),
    imputation_type=nullable(str)
)
def random_forest_model(y, x, covariates={}, oob=True, mtry_fraction=None,
                        min_node_size=None, max_depth=None, seed=None, imputation_type=None):
    """Creates an empty random forest classifier with specified parameters.

    :param Float64Expression y:  Column-indexed response expressions.
    :param Float64Expression x:  Entry-indexed expression for input variable.
    :param covariates: Dictionary of covariate names and their respective expression.
    :param bool oob: Should OOB error be calculated.
    :param float mtry_fraction: The fraction of variables to try at each split.
    :param int min_node_size: The minimum number of samples in a node to be consider for splitting.
    :param int max_depth: The maximum depth of the trees to build.
    :param int seed: Random seed to use.
    :param string imputation_type: Imputation type to use. Currently only "mode" is supported which performs
        basic replacement of missing values with the mode of non missing values. If not provided and input containts
        missing data an error is reported.

    :return: An empty random forest classifier.
    :rtype: :py:class:`RandomForestModel`
    """

    mt = matrix_table_source('random_forest_model/x', x)
    check_entry_indexed('random_forest_model/x', x)

    for key, e in covariates.items():
        analyze('random_forest_model/covariates', e, mt._col_indices)

    '''potential multi-predictions
    y = wrap_to_list(y)
    y_field = ['y']
    y_dict = dict(zip(y_field, y))'''
    y_dict = {'y':y}

    mts = mt._select_all(col_exprs=dict(**y_dict,
                                        **{'cov__'+k:v for k,v in covariates.items()}),
                         row_exprs={},
                         col_key=[],
                         entry_exprs=dict(e=x))


    return rf.RandomForestModel(mts._mir,
                                oob=oob,
                                mtry_fraction=mtry_fraction,
                                min_node_size=min_node_size,
                                max_depth=max_depth,
                                seed=seed,
                                imputation_type=imputation_type)
