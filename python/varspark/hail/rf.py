'''
Created on 10 Nov 2017

@author: szu004
'''
# Ensure backwards compatibility with Python 2
from __future__ import (
    absolute_import,
    division,
    print_function)

from typedecorator import params, Nullable
from hail import  KeyTable

class ImportanceAnalysis(object):
    """ Model for random forest based importance analysis
    """
    def __init__(self, hc, _jia):
        self.hc = hc
        self._jia = _jia

    @property
    def oob_error(self):
        """ OOB (Out of Bag) error estimate for the model

        :rtype: float
        """
        return self._jia.oobError()


    @params(self=object, n_limit=Nullable(int))
    def important_variants(self, n_limit=1000):
        """ Gets the top n most important loci.

        :param int n_limit: the limit of the number of loci to return

        :return: A KeyTable with the variant in the first column and importance in the second.
        :rtype: :py:class:`hail.KeyTable`
        """
        return KeyTable(self.hc, self._jia.variantImportance(n_limit))
