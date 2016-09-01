import numpy as np
import pandas as pd
from scipy.special import expit
import os

BASEDIR =  os.path.abspath(os.path.join(os.path.basename(__file__), '../../../..'))
def proj_path(path):
    return os.path.join(BASEDIR, path)

n_samples = 10000
n_variables = 150000

np.random.seed(13)
data = np.random.randint(0, 3, (n_variables, n_samples), np.int8)
data_df = pd.DataFrame(data)
#data_df.to_csv(proj_path("tmp/ranger-wide_%s_%s.csv" % (n_variables, n_samples)))
resp = expit(np.dot((np.transpose(data[0:5])-1),2*np.ones(5))) >0.5
resp_df = pd.DataFrame(dict(resp5=resp.astype(np.int)))
resp_df.to_csv(proj_path("tmp/ranger-labels_%s_%s.csv" % (n_variables, n_samples)), index = True)

data_narrow = np.transpose(data)
narrow_df = pd.DataFrame(data_narrow)
narrow_df["resp5"] = resp.astype(np.int)
narrow_df.to_csv(proj_path("tmp/ranger_%s_%s.csv" % (n_variables, n_samples)), index = True)
