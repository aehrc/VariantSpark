from typing import Dict
from hail.utils import wrap_to_list
from hail.utils.java import Env


from hail.expr.expressions import *
from hail.typecheck import *


from . import rf





def _get_regression_row_fields(mt, pass_through, method) -> Dict[str, str]:

    row_fields = dict(zip(mt.row_key.keys(), mt.row_key.keys()))
    for f in pass_through:
        if isinstance(f, str):
            if f not in mt.row:
                raise ValueError(f"'{method}/pass_through': MatrixTable has no row field {repr(f)}")
            if f in row_fields:
                # allow silent pass through of key fields
                if f in mt.row_key:
                    pass
                else:
                    raise ValueError(f"'{method}/pass_through': found duplicated field {repr(f)}")
            row_fields[f] = mt[f]
        else:
            assert isinstance(f, Expression)
            if not f._ir.is_nested_field:
                raise ValueError(f"'{method}/pass_through': expect fields or nested fields, not complex expressions")
            if not f._indices == mt._row_indices:
                raise ExpressionException(f"'{method}/pass_through': require row-indexed fields, found indices {f._indices.axes}")
            name = f._ir.name
            if name in row_fields:
                # allow silent pass through of key fields
                if not (name in mt.row_key and f._ir == mt[name]._ir):
                    raise ValueError(f"'{method}/pass_through': found duplicated field {repr(name)}")
            row_fields[name] = f
    for k in mt.row_key:
        del row_fields[k]
    return row_fields



@typecheck(
    y=expr_float64,
    x=expr_int32,
    covariates=sequenceof(expr_float64),
    oob=bool,
    mtry_fraction=nullable(float),
    min_node_size=nullable(int),
    max_depth=nullable(int),
    seed=nullable(int),
    imputation_type=nullable(str)
)
def random_forest_model(y, x, covariates=(), oob=True, mtry_fraction=None,
                        min_node_size=None, max_depth=None, seed=None, imputation_type=None):
    """Creates an empty random forest classifier with specified parameters.

    :param Float64Expression y:  Column-indexed response expressions.
    :param Float64Expression x:  Entry-indexed expression for input variable.
    :param covariates: Reserved for future use.
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
    """
    #ORiginal
    mts = mt._select_all(col_exprs=dict(y=y),
                         row_exprs=dict(),
                         col_key=[],
                         entry_exprs=dict(e=x))"""

    for e in covariates:
        analyze('random_forest_model/covariates', e, mt._col_indices)
    cov_field_names = [f'__cov{i}' for i in range(len(covariates))]
    pass_through=()
    row_fields = _get_regression_row_fields(mt, pass_through, 'random_forest_model')


    y = wrap_to_list(y)

    x_field_name = Env.get_uid()
    y_field = ['y']

    y_dict = dict(zip(y_field, y))


    mts = mt._select_all(col_exprs=dict(**y_dict,
                                        **dict(zip(cov_field_names, covariates))),
                         row_exprs=row_fields,
                         col_key=[],
                         entry_exprs={x_field_name: x})


    return rf.RandomForestModel(mts._mir,
                                oob=oob,
                                mtry_fraction=mtry_fraction,
                                min_node_size=min_node_size,
                                max_depth=max_depth,
                                seed=seed,
                                imputation_type=imputation_type)
