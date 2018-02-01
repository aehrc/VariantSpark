'''
Created on 6 Dec 2017

@author: szu004
'''
# Ensure backwards compatibility with Python 2
from __future__ import (
    absolute_import,
    division,
    print_function)

import numpy as np
import pandas as pd
from pyspark.mllib.linalg.distributed import IndexedRowMatrix, RowMatrix

def dist_mat_to_array(dist_mat):
    """ Converts a (small) distributed  matrix to dense numpy narray

    :param dist_mat: a pyspark.mllib.linalg  distributed matrix
    :return: a local numpy array with the matrix data
    """
    if RowMatrix == type(dist_mat):
        return np.array([v.toArray() for v in dist_mat.rows.collect()])
    elif IndexedRowMatrix == type(dist_mat):
        return dist_mat_to_array(dist_mat.toRowMatrix())
    else:
        raise Exception("Cannot convert distributed matrix of type %s" % type(dist_mat))

def array_to_dataframe(ndarray, labels = None):
    """ Converts a square numpy array to a pandas dataframe with index and column names
    from labels (if provided)

    :param ndarray: a square numpy array to convert
    :param labels: labels to use for the index and for the column names
    :return: a pandas dataframe
    """
    return pd.DataFrame(ndarray, columns = labels, index = labels)

def array_to_dataframe_coord(ndarray, labels = None, triangular = True, include_diagonal = True,
                                row_name = 'row', col_name = 'col', value_name = 'value'):
    """ Converts a square numpy array to a pandas dataframe in coordinate format
    that is `[row, column, value]`. Optionally only includes the lower triangular matrix with
    or without diagonal (to get only unique coordinates)

    :param labels: labels to use for row and columns coordinates
    :param triangular: only include the lower triangular matrix
    :param include_diagonal: if the main diagonal should be included
    :param row_name: the name to use for row column (first coordinate)
    :param col_name: the name to use for col column (second coordinate)
    :param value_name: the name to use for the value column
    :return: dataframe with the values from the kinship matrix in the coordinate form
    """
    pdist_mat = np.array(ndarray)
    if triangular:
        pdist_mat[np.triu_indices(pdist_mat.shape[0], 1 if include_diagonal else 0)] = np.nan
    pdist_df = array_to_dataframe(pdist_mat, labels = labels)
    pdist_df[row_name] = pdist_df.index
    return pd.melt(pdist_df, id_vars = [row_name], var_name = col_name,
                        value_name = value_name).dropna()


def kinship_mat_to_dataframe(km):
    """Converts a hail KinshipMatrix to a pandas dataframe. Index and column names
    are obtained from `sample_list` of the matrix.

    :param km: kinship matrix to convert
    :return: dataframe with the values from the kinship matrix
    """
    return array_to_dataframe(dist_mat_to_array(km.matrix()), labels = km.sample_list())

def kinship_mat_to_dataframe_coord(km, **kwargs):
    """Converts a hail KinshipMatrix to a pandas dataframe. Coordinate values are
    obtained from `sample_list` of the matrix.

    :param km: kinship matrix to convert
    :param kwargs: other conversion parameters as in [[array_to_dataframe_coord]]
    :return: dataframe with the values from the kinship matrix in the coordinate form
    """
    return array_to_dataframe_coord(dist_mat_to_array(km.matrix()),
                                    labels = km.sample_list(), **kwargs)
