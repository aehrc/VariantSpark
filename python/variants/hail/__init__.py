'''
Created on 7 Nov 2017

@author: szu004
'''
from hail import VariantDataset

def importance_analysis(self, expr, root, nTopVariables):
    if not hasattr(self, '_vshf_cache'):
        vsh = getattr(self.hc._jvm, 'au.csiro.variantspark.hail')
        self._vshf_cache = vsh.VSHailFunctions(self._jvds)
    
    return VariantDataset(self.hc, 
        self._vshf_cache.importanceAnalysis(expr, root, nTopVariables))
                
VariantDataset.importance_analysis = importance_analysis