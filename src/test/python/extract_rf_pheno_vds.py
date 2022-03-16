#!/usr/bin/env python

import os
import sys

import hail as hl
from hail.expr.expressions import *

PROJECT_DIR = os.path.abspath(os.path.join(os.path.dirname(sys.argv[0]), '../../..'))


def main():
    hl.init()
    data = hl.import_vcf(os.path.join(PROJECT_DIR, 'data/chr22_1000.vcf'))
    labels = hl.import_table(os.path.join(PROJECT_DIR, 'data/chr22_1000_full_pheno.csv'),
                             delimiter=',',
                             types={
                                 '22_16050408': 'float64',
                                 'age': 'float64',
                                 'bmi': 'float64',
                                 'PC1': 'float64',
                                 'PC2': 'float64',
                                 'PC3': 'float64'
                             }).key_by('sample')

    mt = data.annotate_cols(pheno=labels[data.s])
    col_expressions = dict(
        y=mt.pheno['22_16050408'],
        cov__age=mt.pheno['age'],
        cov__bmi=mt.pheno['bmi'],
        cov__PC1=mt.pheno['PC1'],
        cov__PC2=mt.pheno['PC2'],
        cov__PC3=mt.pheno['PC3']
    )

    x = mt.GT.n_alt_alleles()
    mt = matrix_table_source('random_forest_model/x', x)
    check_entry_indexed('random_forest_model/x', x)
    mts = mt._select_all(col_exprs=col_expressions,
                         row_exprs=dict(),
                         col_key=[],
                         entry_exprs=dict(e=x))

    mts.describe()

    mts.write(
        os.path.join(PROJECT_DIR, 'src/test/data/hail/chr22_1000-22_16050408-pheno.vds'),
        overwrite=True)


if __name__ == "__main__":
    # execute only if run as a script
    main()
