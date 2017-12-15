import unittest

from pyspark.sql import SparkSession
from pyspark.tests import ReusedPySparkTestCase

from variants import VariantsContext


class SampleTestWithPySparkTestCase(ReusedPySparkTestCase):
    def test_variants_context(self):
        self.spark = SparkSession(self.sc)
        vc = VariantsContext(self.spark)

        label = vc.load_label(label_file_path="data/chr22-labels.csv", col_name="22_16050408")
        features = vc.import_vcf(vcf_file_path="data/chr22_1000.vcf")
        imp_analysis = features.importance_analysis(label, 100, float('nan'), True, 13, 100, 3)
        imp_vars = imp_analysis.important_variables(20)
        most_imp_var = imp_vars.head()._1()
        self.assertEqual(most_imp_var, '22_16050408')


if __name__ == '__main__':
    unittest.main(verbosity=2)
