import struct
import pandas as pd
from typedecorator import params, Nullable

class CovariateSource(object):
    def __init__(self, _jvm, _vs_api, _jsql, sql, _jcs):
        self._jcs = _jcs
        self._jvm = _jvm
        self._vs_api = _vs_api
        self._jsql = _jsql
        self.sql = sql