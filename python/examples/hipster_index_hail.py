#!/usr/bin/env python
'''
Created on 24 Jan 2018

@author: szu004
'''
import os

import hail as hl

import varspark.hail as vshl
from pyspark.sql.functions import desc

PROJECT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir))


def main():
    vshl.init()

    data = hl.import_vcf(os.path.join(PROJECT_DIR, 'data/hipsterIndex/hipster.vcf.bgz'))
    labels = hl.import_table(os.path.join(PROJECT_DIR, 'data/hipsterIndex/hipster_labels.txt'),
                             delimiter=',',
                             types=dict(label='float64', score='float64')).key_by('samples')

    mt = data.annotate_cols(hipster=labels[data.s])
    print(mt.count())

    with vshl.random_forest_model(y=mt.hipster.label,
                                  x=mt.GT.n_alt_alleles(), seed=13, mtry_fraction=0.05,
                                  min_node_size=5, max_depth=10) as rf_model:
        rf_model.fit_trees(100, 50)
        print("OOB error: %s" % rf_model.oob_error())
        impTable = rf_model.variable_importance()
        impTable.to_spark(flatten=True).orderBy(desc('importance')).show(100)


if __name__ == '__main__':
    main()
