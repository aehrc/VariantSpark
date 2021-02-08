from hail.backend.spark_backend import SparkBackend

from .context import init, version_info, version
from .hail import SparkBackend__init__
from .methods import *

#
# HACK: Replace the SparkBackend.__init__ with our implementation
# Until we can get the required chanegs to hail
#
SparkBackend.__init__ = SparkBackend__init__

__all__ = ['init',
           'version',
           'version_info',
           'random_forest_model']
