import filecmp
import os
import unittest
import yaml
import pytest

import hail as hl
import numpy.testing as npt
import pandas as pd
import pandas.testing as pdt
from pyspark.sql.functions import *

import varspark.hail as vshl
from varspark.test import PROJECT_DIR

#
# TODO: Add test cases for GRCh38 and missing data imputation
#

@pytest.mark.hail
class RFModelHailTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        vshl.init(master='local[1]', log=os.path.join(PROJECT_DIR, 'target', 'hail.log'),
                  skip_logging_configuration=False, quiet=False)

    @classmethod
    def tearDownClass(cls):
        hl.stop()

    def test_importance_analysis_from_vcf(self):
        with open(os.path.join(PROJECT_DIR, 'src/test/data/hail/chr22_22_16050408-meta.yml'),
                  'r') as meta_in:
            meta_data = yaml.full_load(meta_in)

        expected_oob_error = meta_data['output']['oob_error']
        expected_top100_imp = pd.read_csv(
            os.path.join(PROJECT_DIR, 'src/test/data/hail/chr22_22_16050408-imp.csv'))

        data = hl.import_vcf(os.path.join(PROJECT_DIR, 'data/chr22_1000.vcf'))
        labels = hl.import_table(os.path.join(PROJECT_DIR, 'data/chr22-labels.csv'),
                                 delimiter=',',
                                 types={'22_16050408': 'float64'}).key_by('sample')

        mt = data.annotate_cols(pheno=labels[data.s])
        print(mt.count())
        with vshl.random_forest_model(y=mt.pheno['22_16050408'],
                                      x=mt.GT.n_alt_alleles(), seed=17, mtry_fraction=0.1,
                                      min_node_size=2, max_depth=10) as rf_model:
            rf_model.fit_trees(100, 25)

            impTable = rf_model.variable_importance()
            actual_top100_imp = impTable.to_spark(flatten=False) \
                .select(
                concat(col('locus.contig'), lit('_'),
                       col('locus.position'), lit('_'),
                       array_join(col('alleles'), '_')).alias('variable'),
                col('importance')) \
                .orderBy(desc('importance')) \
                .limit(100) \
                .toPandas()

            rf_model.to_json(os.path.join(PROJECT_DIR, 'target/test-json-model.json'))

            assert filecmp.cmp(
                os.path.join(PROJECT_DIR, 'src/test/data/hail/chr22_22_16050408-model.json'),
                os.path.join(PROJECT_DIR, 'target/test-json-model.json'))
            npt.assert_approx_equal(expected_oob_error, rf_model.oob_error())
            pdt.assert_frame_equal(expected_top100_imp, actual_top100_imp)


if __name__ == '__main__':
    unittest.main(verbosity=2)
