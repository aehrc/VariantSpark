#!/usr/bin/env python

import os
import sys

import hail as hl
from hail.expr.expressions import *

PROJECT_DIR = os.path.abspath(os.path.join(os.path.dirname(sys.argv[0]), '../../..'))


def main():
    hl.init()
    data = hl.import_vcf(os.path.join(PROJECT_DIR, 'data/chr22_1000_missing.vcf'))
    labels = hl.import_table(os.path.join(PROJECT_DIR, 'data/chr22-labels.csv'), delimiter=',',
                             types={'22_16050408': 'float64'}).key_by('sample')

    mt = data.annotate_cols(pheno=labels[data.s])
    y = mt.pheno['22_16050408']
    x = mt.GT.n_alt_alleles()
    mt = matrix_table_source('random_forest_model/x', x)
    check_entry_indexed('random_forest_model/x', x)
    mts = mt._select_all(col_exprs=dict(y=y),
                         row_exprs=dict(),
                         col_key=[],
                         entry_exprs=dict(e=x))

    mts.write(
        os.path.join(PROJECT_DIR, 'src/test/data/hail/chr22_1000_missing-22_16050408.vds'),
        overwrite=True)


if __name__ == "__main__":
    # execute only if run as a script
    main()
