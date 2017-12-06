'''
Created on 6 Dec 2017

@author: szu004
'''

import numpy as np
from pyspark.mllib.linalg.distributed import IndexedRowMatrix, RowMatrix

def dist_mat_to_ndarray(dist_mat):
    """ Converts a (small) distributed  matrix to dense numpy narray
    :param dist_mat: a pyspark.mllib.linalg  distributed matrix
    :return: a local numpy array with the matrix data
    """
    if RowMatrix == type(dist_mat):
        return np.array(dist_mat.rows.map(lambda v:v.toArray()).collect())
    elif IndexedRowMatrix == type(dist_mat):
        return dist_mat_to_ndarray(dist_mat.toRowMatrix())
    else:
        raise Exception("Cannot convert distributed matrix of type %s", type(dist_mat))
