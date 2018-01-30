'''
Created on 7 Nov 2017

@author: szu004
'''
# Ensure backwards compatibility with Python 2
from __future__ import (
    absolute_import,
    division,
    print_function)

from varspark.lang import extend_cls
from hail import VariantDataset
from . extend import VariantsDatasetFunctions

#
# Pimp methods from VariantsDatasetFunctions to VariantDataset
#

extend_cls(VariantDataset, VariantsDatasetFunctions)
