'''
Created on 7 Nov 2017

@author: szu004
'''
# Ensure backwards compatibility with Python 2
from __future__ import (
    absolute_import,
    division,
    print_function)

import sys
from typedecorator import params, Nullable, Union
from hail.java import joption
from hail import KinshipMatrix
from .rf import ImportanceAnalysis

if sys.version_info > (3,):
    long = int


class VariantsDatasetFunctions(object):
    """Extension to hail.VariantDataset with variant-spark related functions
    """

    def __init__(self, *args, **kwargs):
        # check that the VariantDataset fields we rely on
        # have been initialized
        self.hc = self.hc
        self._jvds = self._jvds
        # Create the Java bridge object
        vsh = getattr(self.hc._jvm, 'au.csiro.variantspark.hail')
        self._vshf_cache = vsh.VSHailFunctions(self._jvds)


    @params(self=object, y_expr=str, n_trees=Nullable(int), mtry_fraction=Nullable(float),
            oob=Nullable(bool), seed=Nullable(Union(int, long)), batch_size=Nullable(int))
    def importance_analysis(self, y_expr, n_trees=1000, mtry_fraction=None, oob=True, seed=None,
                      batch_size=100):
        """Builds random forest classifier for the response variable defined with y_expr.

        :param str y_expr: Response expression.  Must evaluate to Boolean or
                numeric with all values 0 or 1.
        :param int n_trees: The number of trees to build in the forest.
        :param float mtry_fraction: The fraction of variables to try at each split.
        :param bool oob: Should OOB error be calculated.
        :param long seed: Random seed to use.
        :param int batch_size: The number of trees to build in one batch.

        :return: Importance analysis model.
        :rtype: :py:class:`ImportanceAnalysis`
        """
        return ImportanceAnalysis(self.hc,
            self._vshf_cache.importanceAnalysis(y_expr, n_trees, joption(mtry_fraction),
                        oob, joption(long(seed) if seed is not None else None),
                        batch_size))

    @params(self=object, operation_name=str)
    def pairwise_operation(self, operation_name):
        """Computes a pairwise operation on encoded genotypes. Currently implemented operations
        include:

        - `manhattan` : the Manhattan distance
        - `euclidean` : the Euclidean distance
        - `sharedAltAlleleCount`: count of shared alternative alleles
        - `anySharedAltAlleleCount`: count of variants that share at least one alternative allele

        :param operation_name: name of the operaiton. One of `manhattan`, `euclidean`,
                `sharedAltAlleleCount`, `anySharedAltAlleleCount`

        :return: A symmetric `no_of_samples x no_of_samples` matrix with the result of
                the pairwise computation.
        :rtype: :py:class:`hail.KinshipMatrix`
        """
        return KinshipMatrix(self._vshf_cache.pairwiseOperation(operation_name))
