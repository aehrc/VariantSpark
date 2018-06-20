# Ensure backwards compatibility with Python 2
from __future__ import (
    absolute_import,
    division,
    print_function)

import os
import sys
import unittest

from pyspark import SparkConf
from pyspark.sql import SparkSession

from varspark import VariantsContext
from varspark.test import find_variants_jar, PROJECT_DIR

if sys.version_info > (3,):
    long = int

THIS_DIR = os.path.dirname(os.path.abspath(__file__))

class VariantSparkPySparkTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        sconf = SparkConf(loadDefaults=False)\
            .set("spark.sql.files.openCostInBytes", 53687091200L)\
            .set("spark.sql.files.maxPartitionBytes", 53687091200L)\
            .set("spark.driver.extraClassPath", find_variants_jar())
        spark = SparkSession.builder.config(conf=sconf)\
            .appName("test").master("local").getOrCreate()
        self.sc = spark.sparkContext

    @classmethod
    def tearDownClass(self):
        self.sc.stop()


class VariantSparkAPITestCase(VariantSparkPySparkTestCase):

    def setUp(self):
        self.spark = SparkSession(self.sc)
        self.vc = VariantsContext(self.spark)

    def test_variants_context_parameter_type(self):
        with self.assertRaises(TypeError) as cm:
            self.vc.load_label(label_file_path=123, col_name=456)
        self.assertEqual('keyword argument label_file_path = 123 doesn\'t match signature str',
                         str(cm.exception))

    def test_importance_analysis_from_vcf(self):
        label_data_path = os.path.join(PROJECT_DIR, 'data/chr22-labels.csv')
        label = self.vc.load_label(label_file_path=label_data_path, col_name='22_16050408')
        feature_data_path = os.path.join(PROJECT_DIR, 'data/chr22_1000.vcf')
        features = self.vc.import_vcf(vcf_file_path=feature_data_path)

        imp_analysis = features.importance_analysis(label, 100, float('nan'), True, 13, 100, 3)
        imp_vars = imp_analysis.important_variables(20)
        most_imp_var = imp_vars[0][0]
        self.assertEqual('22_16050408', most_imp_var)
        df = imp_analysis.variable_importance()
        self.assertEqual('22_16050408',
                         str(df.orderBy('importance', ascending=False).collect()[0][0]))
        oob_error = imp_analysis.oob_error()
        self.assertAlmostEqual(0.016483516483516484, oob_error, 4)


if __name__ == '__main__':
    unittest.main(verbosity=2)
