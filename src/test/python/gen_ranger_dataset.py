import numpy as np
np.random.randint(0, 3, (10, 10), np.int8)
v = np.random.randint(0, 3, (150000, 10000), np.int8)
v
import pandas as pd
pd.DataFrame(v)
dv = pd.DataFrame(v)
dv.to_csv('ranger.csv')
from scipy.special import expit
expit(0)
v * np.zeros(150000)
v * np.zeros(150000).t
np.zeros(150000)
np.dot(v, np.zeros(150000))
v[0:5]
v[0:5].shalw
v[0:5].shape
np.transpose(v[0:5])
np.transpose(v[0:5]) * np.ones(5)
np.dot(np.transpose(v[0:5]),np.ones(5))
np.dot((np.transpose(v[0:5])-1),np.ones(5))
np.dot((np.transpose(v[0:5])-1),2*np.ones(5))
expit(np.dot((np.transpose(v[0:5])-1),2*np.ones(5)))
expit(np.dot((np.transpose(v[0:5])-1),2*np.ones(5)))
expit(np.dot((np.transpose(v[0:5])-1),2*np.ones(5))) >0.5
resp = expit(np.dot((np.transpose(v[0:5])-1),2*np.ones(5))) >0.5
resp.astype(np.int)
respi = resp.astype(np.int)
resp_d = pd.DataFrame(dict(resp5=respi))
resp_d
resp_d.to_csv('labels.csv', index = False)
