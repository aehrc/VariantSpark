import os
import unittest

import pytest
from pyspark import SparkConf
from pyspark.sql import SparkSession

from varspark import VariantsContext, RFModelContext
from varspark.test import find_variants_jar, PROJECT_DIR

THIS_DIR = os.path.dirname(os.path.abspath(__file__))


@pytest.mark.spark
class VariantSparkPySparkTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        sconf = SparkConf(loadDefaults=False) \
            .set("spark.driver.extraClassPath", find_variants_jar())
        spark = SparkSession.builder.config(conf=sconf) \
            .appName("test").master("local").getOrCreate()
        self.sc = spark.sparkContext

    @classmethod
    def tearDownClass(self):
        pass


class VariantSparkAPITestCase(VariantSparkPySparkTestCase):
    # self._ variables are only accessible from other tests if initialised here
    # Would it be better to include model, importance, and fdr definitions here to support multiple unit tests?
    def setUp(self):
        self.spark = SparkSession(self.sc)
        self.vc = VariantsContext(self.spark)

    def test_variants_context_parameter_type(self):
        with self.assertRaises(TypeError) as cm:
            self.vc.load_label(label_file_path=123, col_name=456)
        self.assertEqual('keyword argument label_file_path = 123 doesn\'t match signature str',
                         str(cm.exception))

    def test_rfmodel(self):
        label_data_path = os.path.join(PROJECT_DIR, 'data/chr22-labels.csv')
        label = self.vc.load_label(label_file_path=label_data_path, col_name='22_16050678')
        feature_data_path = os.path.join(PROJECT_DIR, 'data/chr22_1000.vcf')
        features = self.vc.import_vcf(vcf_file_path=feature_data_path)
        rf = RFModelContext(self.spark, mtry_fraction=None, oob=True, seed=17, var_ordinal_levels=3)
        rf.fit_trees(features, label, n_trees=200, batch_size=50)
        imp_analysis = rf.importance_analysis()
        imp_vars = imp_analysis.important_variables(20)
        most_imp_var = imp_vars['variable'][0]
        self.assertEqual('22_16050678_C_T', most_imp_var)
        df = imp_analysis.variable_importance(normalized=True)
        self.assertEqual('22_16050678_C_T',
                         str(df.sort_values(by='importance', ascending=False)['variant_id'].iloc[0]))
        oob_error = rf.oob_error()
        self.assertEqual(0.004578754578754579, oob_error)
        fdrCalc = rf.get_lfdr()
        _, fdr = fdrCalc.compute_fdr(countThreshold = 2, local_fdr_cutoff = 0.05)
        self.assertEqual(0.0002976892628282768, fdr)

if __name__ == '__main__':
    unittest.main(verbosity=2)
