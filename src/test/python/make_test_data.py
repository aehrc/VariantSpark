import os
import pandas as pd

'''
    Generate files for decision tree integrtion test
'''


BASEDIR =  os.path.abspath(os.path.join(os.path.basename(__file__), '../../../..'))
def proj_path(path):
    return os.path.join(BASEDIR, path)


data = pd.read_csv(proj_path('data/CNAE-9.csv'), names = ['category']+ [ "w_%s" % i for i in range(0, 856)])
y = data.category
X = data[[ "w_%s" % i for i in range(0, 856)]]

# Save output data
X_df = X.transpose()
X_df.to_csv(proj_path('data/CNAE-9-wide.csv'))
y_df = pd.DataFrame(y)
y_df.to_csv(proj_path('data/CNAE-9-labels.csv'))


from sklearn import tree
clf = tree.DecisionTreeClassifier(random_state=6)
clf.fit(X,y)
pred = clf.predict(X)

print clf
print "Impurity len: %s" % len(clf.tree_.impurity)

# Save the data for test verification
var_df = pd.DataFrame(dict(importance = clf.feature_importances_), index = X.columns)
pred_df = pd.DataFrame(dict(predicted = pred))
tree_df = pd.DataFrame(dict(impurity=clf.tree_.impurity, feature = clf.tree_.feature, threshold = clf.tree_.threshold))
var_df.to_csv(proj_path('src/test/data/CNAE-9-importance.csv'))
pred_df.to_csv(proj_path('src/test/data/CNAE-9-predicted.csv'))
tree_df.to_csv(proj_path('src/test/data/CNAE-9-tree.csv'))
