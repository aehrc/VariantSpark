'''
Created on 7 Nov 2017

@author: szu004
'''
from hail.java import joption

from .rf import ImportanceAnalysis


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
