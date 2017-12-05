import os
import numpy as np
import pandas as pd
from scipy.spatial.distance import pdist, squareform

'''
    Generate files for pairwise operation tests
'''

BASEDIR = os.path.abspath(os.path.join(os.path.basename(__file__), '../../../..'))

def proj_path(path):
    return os.path.join(BASEDIR, path)


def pwdist(metric):
    return lambda features: squareform(pdist(features, metric=metric))[np.tril_indices(100)]

def anySharedAltCount(features):
    def max_min(v1,v2):
        return np.sum(np.minimum(v1,v2) >0 )  
    result = np.zeros((100,100))
    for r in range(0,100): 
        for c in range(0,r+1):
            result[r,c] = max_min(features_wide_df[r], features_wide_df[c])
    return result[np.tril_indices(100)]

def allSharedAltCount(features):
    def sum_min(v1,v2):
        return np.sum( np.minimum(v1,v2))   
    result = np.zeros((100,100))
    for r in range(0,100): 
        for c in range(0,r+1):
            result[r,c] = sum_min(features_wide_df[r], features_wide_df[c])
    return result[np.tril_indices(100)]
            
            
np.random.seed(13)
features = np.random.randint(3, size=(100,10000))
features_wide_df = pd.DataFrame.from_records(features.T)
features_wide_df.to_csv(proj_path("src/test/data/synthetic_100x10k.csv"))

metrics = {'manhattan':pwdist('cityblock'), 'euclidean':pwdist('euclidean'), 
           'anySharedCount':anySharedAltCount, 'allSharedCount': allSharedAltCount }
pw_metrics = pd.DataFrame.from_items( (m,f(features))for m,f in metrics.items())
pw_metrics.to_csv(proj_path("src/test/data/synthetic_100x10k_metrics.csv"))
