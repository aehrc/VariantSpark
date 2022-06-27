#!/usr/bin/env python
'''
Latest modification on 9 May 2022

@author: szu004
@coauthor: roc
'''
import os

import hail as hl

import varspark.hail as vshl
from pyspark.sql.functions import desc

PROJECT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir))


def main():
    vshl.init()

    data = hl.import_vcf(os.path.join(PROJECT_DIR, 'data/hipsterIndex/hipster.vcf.bgz'))
    labels = hl.import_table(os.path.join(PROJECT_DIR, 'data/hipsterIndex/hipster_labels_covariates.txt'),
                             delimiter=',',
                             types=dict(label='float64', score='float64', age='float64', PC0='float64', PC1='float64', PC2='float64')).key_by('samples')

    mt = data.annotate_cols(hipster=labels[data.s])
    print(mt.count())

    with vshl.random_forest_model(y=mt.hipster.label,
                                  x=mt.GT.n_alt_alleles(), 
                                  covariates={'age':mt.hipster.age, 'PC0':mt.hipster.PC0, 'PC1':mt.hipster.PC1, 'PC2':mt.hipster.PC2},
                                  seed=13, mtry_fraction=0.05,
                                  min_node_size=5, max_depth=10) as rf_model:
        rf_model.fit_trees(100, 50)
        print("OOB error: %s" % rf_model.oob_error())

	# Variable importances
        impTable = rf_model.variable_importance()
        impTable.to_spark(flatten=True).orderBy(desc('importance')).show(100)

        #Cavariate importances 
        covImpTable = rf_model.covariate_importance()
        covImpTable.to_spark(flatten=True).orderBy(desc('importance')).show(4)


if __name__ == '__main__':
    main()
