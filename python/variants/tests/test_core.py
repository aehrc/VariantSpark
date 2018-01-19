import unittest

from pyspark.sql import SparkSession
from pyspark.tests import ReusedPySparkTestCase

from variants import VariantsContext


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
        label = self.vc.load_label(label_file_path="../../../data/chr22-labels.csv",
                                   col_name="22_16050408")
        features = self.vc.import_vcf(vcf_file_path="../../../data/chr22_1000.vcf")
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
