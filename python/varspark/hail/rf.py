'''
Created on 10 Nov 2017

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

from hail.utils import java


class RandomForestModel(object):

    @typecheck_method(
        _mir=MatrixIR,
        oob=bool,
        mtry_fraction=nullable(float),
        min_node_size=nullable(int),
        max_depth=nullable(int),
        seed=nullable(int)
    )
    def __init__(self,_mir, oob=True, mtry_fraction=None, min_node_size=None,
            max_depth=None, seed=None):
        self._mir = _mir
        self._jrf_model = Env.jvm().au.csiro.variantspark.hail.methods.RFModel.pyApply(
            Env.spark_backend('rf')._to_java_ir(self._mir),
            java.joption(mtry_fraction), oob, java.joption(min_node_size),
            java.joption(max_depth), java.joption(seed))

    @typecheck_method(
        n_trees=int,
        batch_size=int
    )
    def fit_trees(self, n_trees = 500, batch_size = 100):
        self._jrf_model.fitTrees(n_trees, batch_size)

    def oob_error(self):
        return self._jrf_model.oobError()

    def variable_importance(self):
        return Table._from_java(self._jrf_model.variableImportance())

    def release(self):
        self._jrf_model.release()
