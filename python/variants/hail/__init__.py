'''
Created on 7 Nov 2017

@author: szu004
'''
from hail import VariantDataset, KeyTable
from hail.java import joption

def _wrap__init__(old__init__):
    def _init(self, *args, **kwargs):
        old__init__(self, *args, **kwargs)
        if not hasattr(self, '_vshf_cache'):
            vsh = getattr(self.hc._jvm, 'au.csiro.variantspark.hail')
            self._vshf_cache = vsh.VSHailFunctions(self._jvds)    
    return _init    


class ImportanceAnalysis:
    
    def __init__(self, hc, _jia):
        self.hc = hc
        self._jia = _jia
        
    @property
    def oob_error(self):
        return self._jia.oobError
    
    def important_variants(self, n_limit = 1000):
        return KeyTable(self.hc, self._jia.variantImportance(n_limit))

class VariantsDatasetFunctions:
    """Extension to hail.VariantDataset with variant-spark related functions
    """
    
    def importance_analysis(self, y_expr, n_trees = 1000, mtry_fraction = None, oob = True, seed = None, 
                      batch_size = 100 ):
        """Builds random forest classifier for the response variable defined with y_expr.
        
            :param str y_expr: Response expression.  Must evaluate to Boolean or
            numeric with all values 0 or 1.
            :param int n_trees: The number of trees to build in the forest.
            :param float mtry_fraction: The fraction of variables to try at each split.
            :param bool oob: Should OOB error be calculated.
            :param float seed: Random seed to use 
            :param int batch_size: The number of trees to build in one batch. 

            :return: Importance Analysis
        """    
        return ImportanceAnalysis(self.hc, 
            self._vshf_cache.importanceAnalysis(y_expr, n_trees, joption(mtry_fraction), 
                                                oob, joption(seed), batch_size))
        
VariantDataset.__init__ = _wrap__init__(VariantDataset.__init__)
VariantDataset.importance_analysis = VariantsDatasetFunctions.importance_analysis.im_func

