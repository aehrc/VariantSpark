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


np.random.seed(13)
features = np.random.randint(2, size=(100,10000))
features_wide_df = pd.DataFrame.from_records(features.T)
features_wide_df.to_csv(proj_path("src/test/data/synthetic_100x10k.csv"))

metrics = ['cityblock', 'euclidean']
pw_metrics = pd.DataFrame.from_items((m,squareform(pdist(features, metric=m))[np.tril_indices(100)]) for m in metrics )
pw_metrics.to_csv(proj_path("src/test/data/synthetic_100x10k_metrics.csv"))