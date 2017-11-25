'''
Created on 7 Nov 2017

@author: szu004
'''
from variants.utils import extend_cls
from hail import VariantDataset
from . extend import VariantsDatasetFunctions

#
# Pimp methods from VariantsDatasetFunctions to VariantDataset
#

extend_cls(VariantDataset, VariantsDatasetFunctions)
