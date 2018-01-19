import os
import unittest

from pyspark.sql import SparkSession
from pyspark.tests import ReusedPySparkTestCase

from variants import VariantsContext

THIS_DIR = os.path.dirname(os.path.abspath(__file__))

class VariantSparkAPITestCase(ReusedPySparkTestCase):

    def setUp(self):
        self.spark = SparkSession(self.sc)
        self.vc = VariantsContext(self.spark)

    def test_variants_context_parameter_type(self):
        with self.assertRaises(TypeError) as cm:
            self.vc.load_label(label_file_path=123, col_name=456)
        self.assertEqual('keyword argument label_file_path = 123 doesn\'t match signature str',
                         str(cm.exception))

    def test_importance_analysis_from_vcf(self):
        label_data_path = os.path.join(THIS_DIR, os.pardir, '../../data/chr22-labels.csv')
        label = self.vc.load_label(label_file_path=label_data_path, col_name='22_16050408')
        feature_data_path = os.path.join(THIS_DIR, os.pardir, '../../data/chr22_1000.vcf')
        features = self.vc.import_vcf(vcf_file_path=feature_data_path)

        imp_analysis = features.importance_analysis(label, 100, float('nan'), True, 13, 100, 3)
        imp_vars = imp_analysis.important_variables(20)
        most_imp_var = imp_vars[0][0]
        self.assertEqual('22_16050408', most_imp_var)
        df = imp_analysis.variable_importance()
        self.assertEqual('22_16050408',
                         str(df.orderBy('importance', ascending=False).collect()[0][0]))
        oob_error = imp_analysis.oob_error()
        self.assertAlmostEqual(0.014652014652014652, oob_error, 3)


if __name__ == '__main__':
    unittest.main(verbosity=2)
