'''
Created on 7 Nov 2017

@author: szu004
'''
# Ensure backwards compatibility with Python 2
from __future__ import (
    absolute_import,
    division,
    print_function)



from typing import *
from hail.expr.expressions import *
from hail.expr.types import *
from hail.typecheck import *
from hail.ir import *
from hail.table import Table
from . import rf

@typecheck(
        y=expr_float64,
        x=expr_int32,
        covariates=sequenceof(expr_float64),
        oob=bool,
        mtry_fraction=nullable(float),
        min_node_size=nullable(int),
        max_depth=nullable(int),
        seed=nullable(int)
        )

def random_forest_model(y, x, covariates=(), oob=True, mtry_fraction=None,
    min_node_size = None, max_depth=None, seed=None):

    mt = matrix_table_source('random_forest_model/x', x)
    check_entry_indexed('random_forest_model/x', x)
    mts = mt._select_all(col_exprs=dict(y=y),
                        row_exprs=dict(),
                        col_key=[],
                        entry_exprs=dict(e=x))
    return rf.RandomForestModel(mts._mir,
                        oob = oob,
                        mtry_fraction=mtry_fraction,
                        min_node_size = min_node_size,
                        max_depth = max_depth,
                        seed = seed)
